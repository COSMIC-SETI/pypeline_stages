import os
import glob
import logging
import traceback
import time

from Pypeline import ProcessNote
from hashpipe_status_keyvalues import HashpipeStatusSharedMemoryIPC

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


STATE_notes = {}
STATE_hpinstance = None
STATE_hpstatus_buffer = None
STATE_recording_exhausted = True
STATE_env = {}
STATE_prev_daq = DaqState.Unknown
STATE_current_daq = DaqState.Unknown
STATE_processed_parts = []
STATE_parts_to_process = []


def setup(hostname, instance, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpinstance
    STATE_hpinstance = instance


def dehydrate():
    global STATE_env, STATE_hpinstance, STATE_hpstatus_buffer, STATE_prev_daq, STATE_current_daq, STATE_processed_parts, STATE_parts_to_process

    return {
        "notes": STATE_notes,
        "instance_id": STATE_hpinstance,
        "env": STATE_env, 
        "hpstatus_buffer": STATE_hpstatus_buffer,
        "prev_daq": STATE_prev_daq, 
        "current_daq": STATE_current_daq,
        "processed_parts": STATE_processed_parts, 
        "parts_to_process": STATE_parts_to_process
    }


def rehydrate(dehydration_dict):
    global STATE_notes, STATE_env, STATE_hpinstance, STATE_hpstatus_buffer, STATE_prev_daq, STATE_current_daq, STATE_processed_parts, STATE_parts_to_process

    STATE_notes = dehydration_dict["notes"]
    STATE_hpinstance = dehydration_dict["instance_id"]
    STATE_env = dehydration_dict["env"]
    STATE_hpstatus_buffer = dehydration_dict["hpstatus_buffer"]
    STATE_prev_daq = dehydration_dict["prev_daq"]
    STATE_current_daq = dehydration_dict["current_daq"]
    STATE_processed_parts = dehydration_dict["processed_parts"]
    STATE_parts_to_process = dehydration_dict["parts_to_process"]


def run(env=None, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    
    global STATE_env, STATE_hpinstance, STATE_hpstatus_buffer, STATE_prev_daq, STATE_current_daq, STATE_processed_parts, STATE_parts_to_process, STATE_recording_exhausted
    # TODO probably ought to only do this when env is a different value
    STATE_env.clear()
    STATE_env.update(common.env_str_to_dict(env))

    STATE_prev_daq = STATE_current_daq
    hpstatus_ipc = HashpipeStatusSharedMemoryIPC(STATE_hpinstance, lock_timeout_s=5)
    STATE_hpstatus_buffer = hpstatus_ipc.parse_buffer()
    daqstate = None
    for i in range(10):
        try:
            daqstate = STATE_hpstatus_buffer["DAQSTATE"]
            break
        except KeyError:
            STATE_hpstatus_buffer = hpstatus_ipc.parse_buffer()
            time.sleep(0.5)
    if daqstate is None:
        logger.warning(f"Could not access DAQSTATE in buffer: {STATE_hpstatus_buffer}")
        return None
    
    if (current_daq := DaqState.decode_daqstate(daqstate)) != DaqState.Unknown:
        STATE_current_daq = current_daq
        if STATE_current_daq != STATE_prev_daq:
            logger.info(f"STATE_current_daq: {STATE_current_daq}, STATE_prev_daq: {STATE_prev_daq}")
    else:
        # logger.warning(f"Unrecognised DAQSTATE value: {daqstate}, previously was {STATE_prev_daq}")
        return None

    # TODO make an exhaustive enum of the transitions...
    record_started = STATE_prev_daq != DaqState.Record and STATE_current_daq == DaqState.Record
    record_ongoing = STATE_current_daq == DaqState.Record
    record_finished = STATE_prev_daq == DaqState.Record and STATE_current_daq == DaqState.Idle
    # logger.debug(f"started: {record_started} , ongoing: {record_ongoing} , finished: {record_finished}")

    STATE_parts_to_process = []
    if not any([record_started, record_ongoing, record_finished]):
        return None

    stem_path = STATE_hpstatus_buffer.observation_stempath
    all_parts = list(
        filter(
            os.path.isfile,
            glob.glob(f"{stem_path}*.????.raw")
        )
    )

    unprocessed_parts = list(
        filter(
            lambda part: part not in STATE_processed_parts,
            all_parts
        )
    )
    unprocessed_parts.sort()
    # unprocessed_parts.sort(key=lambda x: os.path.getmtime(x))
    
    batch_length = max(
        int(STATE_env.get("BATCH_RAWPART_COUNT", 1)),
        1
    )
    
    if record_started:
        # if not STATE_recording_exhausted:
        #     logger.warning(f"New recording started but previous files were not fully processed.")

        # STATE_recording_exhausted = False
        STATE_processed_parts.clear()
        logger.info(f"Recording has started. Initial parts: {all_parts}")
        if len(all_parts) > 1:
            # if more than one part, consider the first complete
            STATE_parts_to_process = all_parts[0:batch_length]
    
    elif record_ongoing:
        if len(unprocessed_parts) > batch_length:
            # new parts mean the previous are complete
            STATE_parts_to_process = unprocessed_parts[0:batch_length]

    elif record_finished:
        logger.info(f"Recording has finished.")
        # remaining unprocessed parts are taken to be complete
        if STATE_env.get("DELETE_FINAL_BATCH", "false").lower() == "true":
            logger.info(f"Deleting final batch: {unprocessed_parts}")
            for part in unprocessed_parts:
                try:
                    os.remove(part)
                except:
                    logger.error(f"Could not delete '{part}'")
        else:
            if len(unprocessed_parts) > 0:
                STATE_parts_to_process = unprocessed_parts[0:]
        if STATE_hpstatus_buffer.get("PKTSTART") == STATE_hpstatus_buffer.get("PKTSTOP"):
            logger.info(f"Recording was cancelled. Not processing remaining parts: {STATE_parts_to_process}")
            STATE_processed_parts += unprocessed_parts
            STATE_parts_to_process = []

    # elif not STATE_recording_exhausted:
    #     STATE_recording_exhausted = len(unprocessed_parts) <= batch_length
    #     logger.info(f"Residual files to process after recording has finished: {unprocessed_parts}.")
    #     # remaining unprocessed parts are taken to be complete
    #     if len(unprocessed_parts) >= 1:
    #         STATE_parts_to_process = unprocessed_parts[0]
    else:
        # not recording and procesed everything
        STATE_parts_to_process = []

    if len(STATE_parts_to_process) > 0:
        STATE_processed_parts.extend(STATE_parts_to_process)

        return STATE_parts_to_process
    else:
        return None


def setupstage(stage, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpstatus_buffer

    if not hasattr(stage, "CONTEXT"):
        logger.debug(f"Stage has no CONTEXT to populate.")
        return
    context_obj = stage.CONTEXT

    for key in context_obj.keys():
        try:
            context_obj[key] = (
                getattr(STATE_hpstatus_buffer, key)
                if hasattr(STATE_hpstatus_buffer, key)
                else STATE_hpstatus_buffer[key]
            )
        except:
            logger.warning(f"Could not populate key: {key}.")


def note(processnote: ProcessNote, **kwargs):
    global STATE_notes, STATE_env, STATE_parts_to_process

    common.context_take_note(STATE_notes, processnote, kwargs)
    logger = kwargs["logger"]

    logger.debug(f"{processnote}")
    if processnote in [ProcessNote.Error, ProcessNote.StageError]:
        if STATE_env.get("POSTPROC_REMOVE_FAILURES", "true").lower() != "false":
            for part_to_process in STATE_parts_to_process:
                try:
                    os.remove(part_to_process)
                    logger.warning(f"Process failed. Removed {part_to_process}.")
                except:
                    logger.error(f"Process failed but could not remove {part_to_process} ({traceback.format_exc()}).")
        else:
            logger.warning(f"Not removing {STATE_parts_to_process}.")

    if processnote in [ProcessNote.Finish, ProcessNote.Error, ProcessNote.StageError]:
        logger.info(common._get_notes_summary(STATE_notes))


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
