import os
import glob
import redis
import logging
from hashpipe_keyvalues.standard import HashpipeKeyValues

NAME = "proc_hpdaq_rawpart"

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


STATE_hpkv = None
STATE_hpkv_cache = None
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
    global STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq, STATE_all_parts, STATE_processed_parts
    return (
        (STATE_hpkv.hostname, STATE_hpkv.instance_id, "redishost"),
        STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq, STATE_all_parts, STATE_processed_parts
    )


def rehydrate(dehydration_tuple):
    global STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq, STATE_all_parts, STATE_processed_parts
    STATE_hpkv = HashpipeKeyValues(
        dehydration_tuple[0][0],
        dehydration_tuple[0][1],
        redis.Redis(dehydration_tuple[1], decode_responses=True)
    )
    STATE_hpkv_cache = dehydration_tuple[1]
    STATE_prev_daq = dehydration_tuple[2]
    STATE_current_daq = dehydration_tuple[3]
    STATE_all_parts = dehydration_tuple[4]
    STATE_processed_parts = dehydration_tuple[5]


def run(logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq, STATE_all_parts, STATE_processed_parts

    STATE_prev_daq = STATE_current_daq
    daqstate = STATE_hpkv.get("DAQSTATE")
    if daqstate is not None:
        STATE_current_daq = DaqState.decode_daqstate(daqstate)
        if STATE_current_daq != STATE_prev_daq:
            logger.info(f"daqstate: {daqstate}, STATE_current_daq: {STATE_current_daq}")

    record_started = STATE_prev_daq != DaqState.Record and STATE_current_daq == DaqState.Record
    record_ongoing = STATE_current_daq == DaqState.Record
    record_finished = STATE_prev_daq == DaqState.Record and STATE_current_daq == DaqState.Idle

    parts_to_process = None
    latest_list_of_parts = glob.glob(f"{os.path.join(*map(str, STATE_hpkv.observation_stempath))}*.raw")
    unseen_latest_parts = [part for part in latest_list_of_parts if part not in STATE_all_parts]
    if record_started:
        # first part is opened, store it and move on
        STATE_all_parts.clear()
        STATE_processed_parts.clear()
        unseen_latest_parts = latest_list_of_parts
        logger.info(f"Recording has started. Initial parts: {latest_list_of_parts}")
    
    elif record_ongoing:
        if len(unseen_latest_parts) > 0:
            # new parts mean the previous are complete
            parts_to_process = [part for part in STATE_all_parts if part not in STATE_processed_parts]

    elif record_finished:
        logger.info(f"Recording has finished.")
        # remaining unprocessed parts are taken to be complete
        parts_to_process = [part for part in STATE_all_parts if part not in STATE_processed_parts]
        if STATE_hpkv.get("PKTSTART") == STATE_hpkv.get("PKTSTOP"):
            logger.info(f"Recording was cancelled. Not processing remaining parts: {parts_to_process}")
            parts_to_process = None
    else:
        # not recording
        return None

    STATE_hpkv_cache = STATE_hpkv.get_cache()
    STATE_all_parts += unseen_latest_parts
    if parts_to_process is not None:
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


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
