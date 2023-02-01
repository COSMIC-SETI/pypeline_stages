import os
import glob
import redis
import logging

from Pypeline import ProcessNote

from hashpipe_keyvalues.standard import HashpipeKeyValues

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


STATE_hpkv = None
STATE_hpkv_cache = None
STATE_prev_daq = DaqState.Unknown
STATE_current_daq = DaqState.Idle


def setup(hostname, instance, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpkv

    STATE_hpkv = HashpipeKeyValues(
        hostname, instance, redis.Redis("redishost", decode_responses=True)
    )


def dehydrate():
    global STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq
    return (
        (STATE_hpkv.hostname, STATE_hpkv.instance_id, "redishost"),
        STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq
    )


def rehydrate(dehydration_tuple):
    global STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq
    STATE_hpkv = HashpipeKeyValues(
        dehydration_tuple[0][0],
        dehydration_tuple[0][1],
        redis.Redis(dehydration_tuple[1], decode_responses=True)
    )
    STATE_hpkv_cache = dehydration_tuple[1]
    STATE_prev_daq = dehydration_tuple[2]
    STATE_current_daq = dehydration_tuple[3]


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

    STATE_hpkv_cache = STATE_hpkv.get_cache()

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
    pass


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
