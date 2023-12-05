import os
import glob
import redis
import logging

from Pypeline import ProcessNote

from hashpipe_keyvalues.standard import HashpipeKeyValues

import common

NAME = "hpdaq"

class DaqState:
    Unknown = -1
    Idle = 0
    Armed = 1
    Record = 2

    @staticmethod
    def decode_daqstate(daqstate_str):
        if daqstate_str == "idling":
            return DaqState.Idle
        if daqstate_str == "armed":
            return DaqState.Armed
        if daqstate_str == "recording":
            return DaqState.Record
        return DaqState.Unknown


STATE_notes = {}
STATE_hpkv = None
STATE_hpkv_cache = None
STATE_env = {}
STATE_prev_daq = DaqState.Unknown
STATE_current_daq = DaqState.Idle
STATE_files_to_process = []


def setup(hostname, instance, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpkv

    STATE_hpkv = HashpipeKeyValues(
        hostname, instance, redis.Redis("redishost", decode_responses=True)
    )


def dehydrate():
    global STATE_notes, STATE_hpkv, STATE_hpkv_cache, STATE_env, STATE_prev_daq, STATE_current_daq, STATE_files_to_process
    return {
        "notes": STATE_notes,
        "hostname": STATE_hpkv.hostname,
        "instance_id": STATE_hpkv.instance_id,
        "env": STATE_env,
        "hpkv_cache": STATE_hpkv_cache,
        "prev_daq": STATE_prev_daq,
        "current_daq": STATE_current_daq,
        "files_to_process": STATE_files_to_process,
    }


def rehydrate(dehydration_dict):
    global STATE_notes, STATE_hpkv, STATE_hpkv_cache, STATE_env, STATE_prev_daq, STATE_current_daq, STATE_files_to_process

    STATE_notes = dehydration_dict["notes"]
    STATE_hpkv = HashpipeKeyValues(
        dehydration_dict["hostname"],
        dehydration_dict["instance_id"],
        redis.Redis("redishost", decode_responses=True)
    )
    STATE_env = dehydration_dict["env"]
    STATE_hpkv_cache = dehydration_dict["hpkv_cache"]
    STATE_prev_daq = dehydration_dict["prev_daq"]
    STATE_current_daq = dehydration_dict["current_daq"]
    STATE_files_to_process = dehydration_dict["files_to_process"]


def run(env=None, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq

    STATE_prev_daq = STATE_current_daq
    daqstate = STATE_hpkv.get("DAQSTATE")
    if daqstate is not None:
        STATE_current_daq = DaqState.decode_daqstate(daqstate)
        if STATE_current_daq != STATE_prev_daq:
            logger.info(f"daqstate: {daqstate}, STATE_current_daq: {STATE_current_daq}")

    record_finished = STATE_prev_daq == DaqState.Record and STATE_current_daq == DaqState.Idle
    if not record_finished:
        return None
    elif STATE_hpkv.get("PKTSTART") == 0 and STATE_hpkv.get("PKTSTOP") == 0:
        # seems that the observation aborted, ignore
        return None

    # prev_daq == DaqState.Record and current_daq = DaqState.Idle
    # i.e. recording just completed
    obs_stempath = f"{os.path.join(*STATE_hpkv.observation_stempath)}*"
    output_filepaths = glob.glob(obs_stempath)

    STATE_hpkv_cache = STATE_hpkv.get()

    return output_filepaths


def setupstage(stage, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpkv_cache
    context_obj = None
    if hasattr(stage, "CONTEXT"):
        context_obj = stage.CONTEXT
    elif hasattr(stage, "PROC_CONTEXT"):
        context_obj = stage.PROC_CONTEXT
    else:
        logger.warning(f"Stage has no CONTEXT to populate.")
        return

    for key in context_obj.keys():
        try:
            context_obj[key] = (
                getattr(STATE_hpkv_cache, key)
                if hasattr(STATE_hpkv_cache, key)
                else STATE_hpkv_cache.get(key)
            )
        except:
            logger.error(f"Could not populate key: {key}.")


def note(processnote: ProcessNote, **kwargs):
    global STATE_notes, STATE_env, STATE_hpkv, STATE_hpkv_cache, STATE_files_to_process

    common.context_take_note(STATE_notes, processnote, kwargs)

    if processnote == ProcessNote.Error:
        if STATE_env.get("POSTPROC_REMOVE_FAILURES", "true").lower() != "false":

            for file_to_process in STATE_files_to_process:
                try:
                    os.remove(file_to_process)
                    logger.warning(f"Process failed. Removed {file_to_process}.")
                except:
                    logger.error(f"Process failed but could not remove {file_to_process} ({traceback.format_exc()}).")
        else:
            logger.warning(f"Not removing {STATE_files_to_process}.")

    if "POSTPROC_PROGRESS_REDIS_CHANNEL" not in STATE_env:
        return

    progress_statement = {
        "hostname": STATE_hpkv.hostname,
        "instance_id": STATE_hpkv.instance_id,
        "observation_id": STATE_hpkv_cache.get("OBSID"),
        "process_id": str(kwargs["process_id"]),
    }
    common.context_build_statement_of_note(progress_statement, processnote, kwargs)
    kwargs["logger"].debug(progress_statement)

    redis_channel = STATE_env["POSTPROC_PROGRESS_REDIS_CHANNEL"]
    redis_obj = STATE_hpkv.redis_obj

    redis_obj.publish(
        redis_channel,
        json.dumps(progress_statement)
    )


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
