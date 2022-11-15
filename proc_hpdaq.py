import os
import glob
import redis
from hashpipe_keyvalues.standard import HashpipeKeyValues


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


def setup(hostname, instance):
    global STATE_hpkv

    STATE_hpkv = HashpipeKeyValues(
        hostname, instance, redis.Redis("redishost", decode_responses=True)
    )


def run():
    global STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq

    STATE_prev_daq = STATE_current_daq
    daqstate = STATE_hpkv.get("DAQSTATE")
    if daqstate is not None:
        STATE_current_daq = DaqState.decode_daqstate(daqstate)
        if STATE_current_daq != STATE_prev_daq:
            print(daqstate, STATE_current_daq)

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


def setupstage(stage):
    global STATE_hpkv_cache
    if hasattr(stage, "PROC_CONTEXT"):
        for key in stage.PROC_CONTEXT.keys():
            stage.PROC_CONTEXT[key] = (
                getattr(STATE_hpkv_cache, key)
                if hasattr(STATE_hpkv_cache, key)
                else STATE_hpkv_cache.get(key)
            )


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
