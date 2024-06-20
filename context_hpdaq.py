import os
import glob
import traceback
import logging
import time

from Pypeline import ProcessNote
from hashpipe_status_keyvalues import HashpipeStatusSharedMemoryIPC

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
STATE_hpinstance = None
STATE_hpstatus_buffer = None
STATE_env = {}
STATE_prev_daq = DaqState.Unknown
STATE_current_daq = DaqState.Idle
STATE_files_to_process = []


def setup(hostname, instance, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpinstance
    STATE_hpinstance = instance


def dehydrate():
    global STATE_notes, STATE_hpinstance, STATE_hpstatus_buffer, STATE_env, STATE_prev_daq, STATE_current_daq, STATE_files_to_process

    return {
        "notes": STATE_notes,
        "instance_id": STATE_hpinstance,
        "env": STATE_env,
        "hpstatus_buffer": STATE_hpstatus_buffer,
        "prev_daq": STATE_prev_daq,
        "current_daq": STATE_current_daq,
        "files_to_process": STATE_files_to_process,
    }


def rehydrate(dehydration_dict):
    global STATE_notes, STATE_hpinstance, STATE_hpstatus_buffer, STATE_env, STATE_prev_daq, STATE_current_daq, STATE_files_to_process

    STATE_notes = dehydration_dict["notes"]
    STATE_hpinstance = dehydration_dict["instance_id"]
    STATE_env = dehydration_dict["env"]
    STATE_hpstatus_buffer = dehydration_dict["hpstatus_buffer"]
    STATE_prev_daq = dehydration_dict["prev_daq"]
    STATE_current_daq = dehydration_dict["current_daq"]
    STATE_files_to_process = dehydration_dict["files_to_process"]


def run(env=None, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpinstance, STATE_hpstatus_buffer, STATE_prev_daq, STATE_current_daq

    STATE_prev_daq = STATE_current_daq
    hpstatus_ipc = HashpipeStatusSharedMemoryIPC(STATE_hpinstance)
    STATE_hpstatus_buffer = hpstatus_ipc.parse_buffer()
    daqstate = None
    for i in range(5):
        try:
            daqstate = STATE_hpstatus_buffer["DAQSTATE"]
            break
        except KeyError:
            STATE_hpstatus_buffer = hpstatus_ipc.parse_buffer()
            time.sleep(0.1)
    if daqstate is None:
        logger.warning(f"Could not access DAQSTATE in buffer: {STATE_hpstatus_buffer}")
        return None
        
    if (current_daq := DaqState.decode_daqstate(daqstate)) != DaqState.Unknown:
        STATE_current_daq = current_daq
        if STATE_current_daq != STATE_prev_daq:
            logger.info(f"STATE_current_daq: {STATE_current_daq}, STATE_prev_daq: {STATE_prev_daq}")
    else:
        logger.warning(f"Unrecognised DAQSTATE value: {daqstate}, previously was {STATE_prev_daq}")
        return None

    record_finished = STATE_prev_daq == DaqState.Record and STATE_current_daq == DaqState.Idle
    if not record_finished:
        return None
    elif STATE_hpstatus_buffer.get("PKTSTART") == 0 and STATE_hpstatus_buffer.get("PKTSTOP") == 0:
        logger.warning(f"PKTSTART and PKTSTOP are zero, ignoring observation products.")
        return None

    # prev_daq == DaqState.Record and current_daq = DaqState.Idle
    # i.e. recording just completed
    for i in range(5):
        logger.debug(f"{STATE_hpstatus_buffer}")
        try:
            obs_stempath = f"{STATE_hpstatus_buffer.observation_stempath}*"
            break
        except:
            obs_stempath = None

        STATE_hpstatus_buffer = hpstatus_ipc.parse_buffer()
    assert obs_stempath is not None, f"Could not gather observation_stempath: {STATE_hpstatus_buffer}"
        
    logger.info(f"{obs_stempath}")
    output_filepaths = glob.glob(obs_stempath)
    logger.debug(f"{output_filepaths}")

    return output_filepaths


def setupstage(stage, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpstatus_buffer
    context_obj = None
    if hasattr(stage, "CONTEXT"):
        context_obj = stage.CONTEXT
    else:
        logger.warning(f"Stage has no CONTEXT to populate.")
        return

    for key in context_obj.keys():
        try:
            context_obj[key] = (
                getattr(STATE_hpstatus_buffer, key)
                if hasattr(STATE_hpstatus_buffer, key)
                else STATE_hpstatus_buffer[key]
            )
        except:
            logger.error(f"Could not populate key: {key}.")


def note(processnote: ProcessNote, **kwargs):
    global STATE_notes, STATE_env, STATE_files_to_process

    common.context_take_note(STATE_notes, processnote, kwargs)
    logger = kwargs["logger"]

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

    if processnote == ProcessNote.Finish or processnote == ProcessNote.Error:
        logger.info(common._get_notes_summary(STATE_notes))


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
