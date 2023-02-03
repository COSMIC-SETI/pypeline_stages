import os
import glob
import json
import logging
import traceback

import redis

from Pypeline import ProcessNote

from hashpipe_keyvalues.standard import HashpipeKeyValues

import common

NAME = "hpdaq_rawpart"

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

    @staticmethod
    def encode_daqstate(daqstate):
        if daqstate == DaqState.Idle:
            return "idling"
        if daqstate == DaqState.Armed:
            return "armed"
        if daqstate == DaqState.Record:
            return "recording"
        return "unknown"


STATE_hpkv = None
STATE_hpkv_cache = None
STATE_env = {}
STATE_prev_daq = DaqState.Unknown
STATE_current_daq = DaqState.Idle
STATE_all_parts = []
STATE_processed_parts = []


def setup(hostname, instance, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpkv

    STATE_hpkv = HashpipeKeyValues(
        hostname, instance, redis.Redis("redishost", decode_responses=True)
    )


def dehydrate():
    global STATE_env, STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq, STATE_all_parts, STATE_processed_parts
    return (
        (STATE_hpkv.hostname, STATE_hpkv.instance_id, "redishost"),
        STATE_env, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq, STATE_all_parts, STATE_processed_parts
    )


def rehydrate(dehydration_tuple):
    global STATE_env, STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq, STATE_all_parts, STATE_processed_parts
    STATE_hpkv = HashpipeKeyValues(
        dehydration_tuple[0][0],
        dehydration_tuple[0][1],
        redis.Redis(dehydration_tuple[1], decode_responses=True)
    )
    STATE_env = dehydration_tuple[1]
    STATE_hpkv_cache = dehydration_tuple[2]
    STATE_prev_daq = dehydration_tuple[3]
    STATE_current_daq = dehydration_tuple[4]
    STATE_all_parts = dehydration_tuple[5]
    STATE_processed_parts = dehydration_tuple[6]


def run(env=None, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    
    global STATE_env, STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq, STATE_all_parts, STATE_processed_parts
    # TODO probably ought to only do this when env is a different value
    STATE_env.clear()
    STATE_env.update(common.env_str_to_dict(env))

    daqstate = STATE_hpkv.get("DAQSTATE")
    if daqstate is not None:
        STATE_prev_daq = STATE_current_daq
        STATE_current_daq = DaqState.decode_daqstate(daqstate)
        if STATE_current_daq != STATE_prev_daq:
            logger.info(f"daqstate: {daqstate}, STATE_current_daq: {STATE_current_daq}, STATE_prev_daq: {DaqState.encode_daqstate(STATE_prev_daq)}")
    else:
        logger.warning("DaqState is None.")

    record_started = STATE_prev_daq != DaqState.Record and STATE_current_daq == DaqState.Record
    record_ongoing = STATE_current_daq == DaqState.Record
    record_finished = STATE_prev_daq == DaqState.Record and STATE_current_daq == DaqState.Idle
    # logger.debug(f"started: {record_started} , ongoing: {record_ongoing} , finished: {record_finished}")

    parts_to_process = None
    latest_list_of_parts = list(
        filter(
            os.path.isfile,
            glob.glob(f"{os.path.join(*map(str, STATE_hpkv.observation_stempath))}*.raw")
        )
    )
    latest_list_of_parts.sort(key=lambda x: os.path.getmtime(x))

    novel_parts = [part for part in latest_list_of_parts if part not in STATE_all_parts]
    unprocessed_parts = [part for part in STATE_all_parts if part not in STATE_processed_parts]
    
    if record_started:
        # first part is opened, store it and move on
        STATE_all_parts.clear()
        STATE_processed_parts.clear()
        novel_parts = latest_list_of_parts
        logger.info(f"Recording has started. Initial parts: {latest_list_of_parts}")
        if len(novel_parts) > 1:
            parts_to_process = novel_parts[0]
    
    elif record_ongoing:
        if len(novel_parts) > 0 or len(unprocessed_parts) > 1:
            # new parts mean the previous are complete
            parts_to_process = unprocessed_parts

    elif record_finished:
        logger.info(f"Recording has finished.")
        # remaining unprocessed parts are taken to be complete
        parts_to_process = unprocessed_parts
        if STATE_hpkv.get("PKTSTART") == STATE_hpkv.get("PKTSTOP"):
            logger.info(f"Recording was cancelled. Not processing remaining parts: {parts_to_process}")
            parts_to_process = None
    elif len(unprocessed_parts) > 0:
        logger.info(f"Residual files to process after recording has finished: {len(unprocessed_parts)}.")
        # remaining unprocessed parts are taken to be complete
        parts_to_process = unprocessed_parts
    else:
        # not recording and procesed everything
        return None

    STATE_hpkv_cache = STATE_hpkv.get_cache()
    STATE_all_parts += novel_parts
    if parts_to_process is not None:
        # take one at a time
        parts_to_process = parts_to_process[0:1]
        STATE_processed_parts += parts_to_process

    return parts_to_process


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
    global STATE_env, STATE_hpkv, STATE_hpkv_cache

    if "POSTPROC_PROGRESS_REDIS_CHANNEL" not in STATE_env:
        return

    redis_channel = STATE_env["POSTPROC_PROGRESS_REDIS_CHANNEL"]
    redis_obj = STATE_hpkv.redis_obj

    progress_statement = {
        "hostname": STATE_hpkv.hostname,
        "instance_id": STATE_hpkv.instance_id,
        "observation_id": STATE_hpkv_cache.get("OBSID"),
        "process_id": kwargs["process_id"],
    }
    try:
        progress_statement["process_note"] = ProcessNote.string(processnote)
    except:
        progress_statement["process_note"] = "Unknown"

    if processnote == ProcessNote.Start:
        progress_statement["context_outputs"] = kwargs["context_outputs"]
    elif processnote == ProcessNote.StageStart:
        progress_statement["stage_name"] = kwargs["stage"].NAME
        progress_statement["stage_inputs"] = kwargs["inpvalue"]
        progress_statement["stage_arguments"] = kwargs["argvalue"]
        progress_statement["stage_environment"] = kwargs["envvalue"]
    elif processnote == ProcessNote.StageFinish:
        progress_statement["stage_name"] = kwargs["stage"].NAME
        progress_statement["stage_output"] = kwargs["output"]
    elif processnote == ProcessNote.StageError:
        progress_statement["stage_name"] = kwargs["stage"].NAME
        progress_statement["error"] = repr(kwargs["error"])
        progress_statement["traceback"] = traceback.format_exc()
    elif processnote == ProcessNote.Finish:
        pass

    redis_obj.publish(
        redis_channel,
        json.dumps(progress_statement)
    )


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
