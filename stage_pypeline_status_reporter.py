from typing import List, Dict
import time
import logging
import argparse
import os
from enum import Enum

from hashpipe_keyvalues.standard import HashpipeKeyValues
from cosmic.observations.slackbot import SlackBot

from Pypeline.redis_interface import RedisClientInterface
from Pypeline.dataclasses import ServiceIdentifier, JobEvent, ProcessNote

import common

ENV_KEY = None
ARG_KEY = "PypelineStatusReporterARG"
INP_KEY = "PypelineStatusReporterINP"
NAME = "pypeline_status_reporter"

STAGE_CONTEXT = None

class JobCompletionStatus(str, Enum):
    Dropped = "dropped"
    Skipped = "skipped"
    Errored = "errored"
    Finished = "finished"
    Queued = "queued"

def service_identifier_from_hashpipe_target_string(hpt_str):
    last_fullstop_index = hpt_str.rindex('.')
    return ServiceIdentifier(
        hostname=hpt_str[0:last_fullstop_index],
        enumeration=int(hpt_str[last_fullstop_index+1:])
    )

def post_alert(slackbot, message_blocks: list, channel_id: str = None, message_ts: str = None):
    if message_ts is None:
        slackbot.post_message(
            *message_blocks,
            channel=channel_id
        )
        return slackbot.last_message_ts

    slackbot.update_message(
        *message_blocks,
        ts=message_ts
    )
    return message_ts

def run(argstr, inputs, env, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)

    slackbot_token = os.environ.get("SLACK_BOT_TOKEN", None)
    if slackbot_token is None:
        message = f"Expecting the slackbot token under environment variable 'SLACK_BOT_TOKEN'."
        logger.error(message)
        raise ValueError(message)

    slackbot = SlackBot(slackbot_token)

    if len(inputs) != 1 or not isinstance(inputs[0], dict):
        message = "Expecting a single input, the observation message dictionary."
        logger.error(message)
        raise ValueError(message)

    observation_message = inputs[0]
    logger.debug(f"observation_message: {observation_message}")

    parser = argparse.ArgumentParser(
        description="A stage that that monitors pypeline instances for process status updates.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--obs-end-margin-s",
        default=5,
        type=int,
        help="How long to after the end of the observation to keep monitoring."
    )
    parser.add_argument(
        "--slack-update-limit-s",
        default=0.5,
        type=float,
        help="The minimal period between slack updates."
    )
    parser.add_argument(
        "--slack-alerts-channel-id",
        type=str,
        help="The channel ID to which alerts about failures will be sent."
    )
    args = parser.parse_args(argstr.split(" "))

    env_dict = {}
    if env is not None:
        # must set ENV_KEY to trigger this
        env_dict = common.env_str_to_dict(env)

    deadline_unix = (
        observation_message["start_epoch_seconds"]
        + observation_message["duration_seconds"]
        + args.obs_end_margin_s
    )

    hashpipe_instances = observation_message["hashpipe_instances"]
    hashpipe_instances.sort()
    service_identifiers: List[ServiceIdentifier] = [
        service_identifier_from_hashpipe_target_string(hpt_str)
        for hpt_str in hashpipe_instances
    ]

    pypeline_redis_clients = {
        si: RedisClientInterface(
            si,
            host=env_dict.get("REDIS_HOSTNAME", "redishost"),
            port=int(env_dict.get("REDIS_PORT", 6379)),
            timeout_s=0.001
        )
        for si in service_identifiers
    }

    pypeline_queued_jobids: Dict[ServiceIdentifier, List[int]] = {
        si: []
        for si in service_identifiers
    }

    pypeline_busy_processids: Dict[ServiceIdentifier, List[int]] = {
        si: []
        for si in service_identifiers
    }

    pypeline_jobcompletionstatuses: Dict[ServiceIdentifier, Dict[JobCompletionStatus, int]] = {
        si: {}
        for si in service_identifiers
    }

    pypline_alertmessages: Dict[ServiceIdentifier, List[str]] = {
        si: []
        for si in service_identifiers
    }

    slack_message_result = slackbot.post_message(
        "Pypeline status reporter",
        thread_ts=observation_message["slack_bot_threadts"]
    )
    logger.debug(f"slack_message_result @ ({observation_message['slack_bot_threadts']}): {slack_message_result} vs {slackbot.last_message_ts}")
    slack_message_ts = slackbot.last_message_ts
    message_link = slackbot.client.chat_getPermalink(
        channel=slack_message_result["channel"],
        message_ts=slack_message_result["ts"],
    )
    message_referal_str = f"<{message_link['permalink']}>"
    
    slack_alert_ts = None
    message_update = False
    alert_update = False
    slack_update_allowed_timestamp = time.time() + args.slack_update_limit_s

    continue_monitoring = True
    while continue_monitoring:
        continue_monitoring = False

        for si, client in pypeline_redis_clients.items():
            if time.time() < deadline_unix:
                continue_monitoring = True
                # note queued jobs
                # logger.info(f"Queurying job event messages for: {si}")
                if (job_event_message := client.job_event_message) is not None:
                    logger.debug(f"job_event_message: {job_event_message}")
                    if job_event_message.event == JobEvent.Skip:
                        pypeline_jobcompletionstatuses[si][JobCompletionStatus.Skipped] = (
                            pypeline_jobcompletionstatuses[si].get(JobCompletionStatus.Skipped, 0) + 1
                        )
                        message_update = True
                    elif job_event_message.event == JobEvent.Drop:
                        pypeline_jobcompletionstatuses[si][JobCompletionStatus.Dropped] = (
                            pypeline_jobcompletionstatuses[si].get(JobCompletionStatus.Dropped, 0) + 1
                        )
                        message_update = True
                        alert_update = True
                        pypline_alertmessages[si].append(
                            "Dropped"
                        )
                    else:
                        message_update = True
                        assert job_event_message.event == JobEvent.Queue
                        pypeline_queued_jobids[si].append(
                            job_event_message.job_parameters.job_id
                        )

            if len(pypeline_queued_jobids[si]) + len(pypeline_busy_processids[si]) > 0:
                # get notes on queued jobs
                continue_monitoring = True
                # logger.info(f"Queurying process note messages for: {si}")
                if (process_note_message := client.process_note_message) is not None:
                    logger.debug(f"process_note_message: {process_note_message}")
                    if process_note_message.process_note == ProcessNote.Start:
                        index = pypeline_queued_jobids[si].index(process_note_message.job_id)
                        pypeline_queued_jobids[si].pop(index)

                        pypeline_busy_processids[si].append(process_note_message.process_id)
                        message_update = True
                        
                    elif process_note_message.process_note == ProcessNote.Finish:
                        index = pypeline_busy_processids[si].index(process_note_message.process_id)
                        pypeline_busy_processids[si].pop(index)

                        pypeline_jobcompletionstatuses[si][JobCompletionStatus.Finished] = (
                            pypeline_jobcompletionstatuses[si].get(JobCompletionStatus.Finished, 0) + 1
                        )
                        message_update = True
                    elif process_note_message.process_note == ProcessNote.Error:
                        index = pypeline_busy_processids[si].index(process_note_message.process_id)
                        pypeline_busy_processids[si].pop(index)

                        pypeline_jobcompletionstatuses[si][JobCompletionStatus.Errored] = (
                            pypeline_jobcompletionstatuses[si].get(JobCompletionStatus.Errored, 0) + 1
                        )
                        message_update = True
                        alert_update = True
                        pypline_alertmessages[si].append(
                            f"Errored: {process_note_message.error_message}"
                        )
                    elif process_note_message.job_id in pypeline_queued_jobids[si]:
                        logger.warn(f"... unexpected note for {process_note_message.job_id}: {process_note_message.process_note}")
                        index = pypeline_queued_jobids[si].index(process_note_message.job_id)
                        pypeline_queued_jobids[si].pop(index)
                    elif process_note_message.process_note == ProcessNote.StageError:
                        index = pypeline_busy_processids[si].index(process_note_message.process_id)
                        pypeline_busy_processids[si].pop(index)

                        pypeline_jobcompletionstatuses[si][JobCompletionStatus.Errored] = (
                            pypeline_jobcompletionstatuses[si].get(JobCompletionStatus.Errored, 0) + 1
                        )
                        message_update = True
                        alert_update = True
                        pypline_alertmessages[si].append(
                            f"StageErrored: {process_note_message.error_message}"
                        )

        # post update
        if (not continue_monitoring) and message_update:
            sleep_time = slack_update_allowed_timestamp - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)
        
        if message_update and (time.time() >= slack_update_allowed_timestamp):
            message_sections=[]
            for si, jcss in pypeline_jobcompletionstatuses.items():
                statement = "Nothing completed..."
                if len(jcss) > 0: 
                    statement = '\n'.join(f"{k}: {v}" for k,v in jcss.items())
                message_sections.append(
                    f"{si} ({len(pypeline_queued_jobids[si])} Queued, {len(pypeline_busy_processids[si])} Busy)\n```{statement}```\n"
                )

            response = slackbot.update_message(
                "Post-Process is " + ("ongoing..." if continue_monitoring else "complete."),
                "\n".join(message_sections),
                ts=slack_message_ts
            )
            message_update = False
            slack_update_allowed_timestamp = time.time() + args.slack_update_limit_s

        if (not continue_monitoring) and alert_update:
            sleep_time = slack_update_allowed_timestamp - time.time()
            if sleep_time > 0:
                time.sleep(sleep_time)

        if alert_update and (time.time() >= slack_update_allowed_timestamp):
            slack_alert_ts = post_alert(
                slackbot,
                [
                    "Post-Process is " + ("ongoing..." if continue_monitoring else "complete."),
                    message_referal_str,
                    "\n".join([
                        f"{si}\n```"+'\n'.join(ms)+"```\n"
                        for si, ms in pypline_alertmessages.items()
                    ])
                ],
                channel_id=args.slack_alerts_channel_id,
                message_ts=slack_alert_ts
            )
            alert_update = False
            slack_update_allowed_timestamp = time.time() + args.slack_update_limit_s

    logger.info("finished")
    return []