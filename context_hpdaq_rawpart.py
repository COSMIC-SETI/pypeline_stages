import os
import glob
import json
import logging
import traceback
import time

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


STATE_notes = {}
STATE_hpkv = None
STATE_hpkv_cache = None
STATE_recording_exhausted = True
STATE_env = {}
STATE_prev_daq = DaqState.Unknown
STATE_current_daq = DaqState.Idle
STATE_all_parts = []
STATE_processed_parts = []
STATE_part_to_process = []


def setup(hostname, instance, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpkv

    STATE_hpkv = HashpipeKeyValues(
        hostname, instance, redis.Redis("redishost", decode_responses=True)
    )


def dehydrate():
    global STATE_env, STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq, STATE_all_parts, STATE_processed_parts, STATE_part_to_process
    return {
        "notes": STATE_notes,
        "hostname": STATE_hpkv.hostname,
        "instance_id": STATE_hpkv.instance_id,
        "env": STATE_env, 
        "hpkv_cache": STATE_hpkv_cache, 
        "prev_daq": STATE_prev_daq, 
        "current_daq": STATE_current_daq, 
        "all_parts": STATE_all_parts, 
        "processed_parts": STATE_processed_parts, 
        "part_to_process": STATE_part_to_process
    }


def rehydrate(dehydration_dict):
    global STATE_notes, STATE_env, STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq, STATE_all_parts, STATE_processed_parts, STATE_part_to_process

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
    STATE_all_parts = dehydration_dict["all_parts"]
    STATE_processed_parts = dehydration_dict["processed_parts"]
    STATE_part_to_process = dehydration_dict["part_to_process"]


def run(env=None, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    
    global STATE_env, STATE_hpkv, STATE_hpkv_cache, STATE_prev_daq, STATE_current_daq, STATE_all_parts, STATE_processed_parts, STATE_part_to_process, STATE_recording_exhausted
    # TODO probably ought to only do this when env is a different value
    STATE_env.clear()
    STATE_env.update(common.env_str_to_dict(env))

    daqstate = STATE_hpkv.get("DAQSTATE")
    if daqstate is not None:
        STATE_prev_daq = STATE_current_daq
        STATE_current_daq = DaqState.decode_daqstate(daqstate)
        if STATE_current_daq != STATE_prev_daq:
            logger.info(f"daqstate: {daqstate}, STATE_current_daq: {STATE_current_daq}, STATE_prev_daq: {DaqState.encode_daqstate(STATE_prev_daq)}")
    elif STATE_prev_daq != DaqState.Unknown:
        logger.warning(f"DaqState is None, previously was {STATE_current_daq}")
        return None

    record_started = STATE_prev_daq != DaqState.Record and STATE_current_daq == DaqState.Record
    record_ongoing = STATE_current_daq == DaqState.Record
    record_finished = STATE_prev_daq == DaqState.Record and STATE_current_daq == DaqState.Idle
    # logger.debug(f"started: {record_started} , ongoing: {record_ongoing} , finished: {record_finished}")

    STATE_part_to_process = None
    stem_path = os.path.join(*map(str, STATE_hpkv.observation_stempath))
    STATE_all_parts = list(
        filter(
            os.path.isfile,
            glob.glob(f"{stem_path}*.????.raw")
        )
    )
    STATE_all_parts.sort(key=lambda x: os.path.getmtime(x))

    unprocessed_parts = list(
        filter(
            lambda part: part not in STATE_processed_parts,
            STATE_all_parts
        )
    )
    
    if record_started:
        if not STATE_recording_exhausted:
            logger.warning(f"New recording started but previous files were not fully processed.")

        STATE_recording_exhausted = False
        STATE_processed_parts.clear()
        logger.info(f"Recording has started. Initial parts: {STATE_all_parts}")
        if len(STATE_all_parts) > 1:
            # if more than one part, consider the first complete
            STATE_part_to_process = STATE_all_parts[0]
    
    elif record_ongoing:
        if len(unprocessed_parts) > 1:
            # new parts mean the previous are complete
            STATE_part_to_process = unprocessed_parts[0]

    elif record_finished:
        logger.info(f"Recording has finished.")
        # remaining unprocessed parts are taken to be complete
        if len(unprocessed_parts) > 0:
            STATE_part_to_process = unprocessed_parts[0]
        if STATE_hpkv.get("PKTSTART") == STATE_hpkv.get("PKTSTOP"):
            logger.info(f"Recording was cancelled. Not processing remaining parts: {STATE_part_to_process}")
            STATE_processed_parts += unprocessed_parts
            STATE_part_to_process = None

    elif not STATE_recording_exhausted:
        STATE_recording_exhausted = len(unprocessed_parts) <= 1
        logger.info(f"Residual files to process after recording has finished: {unprocessed_parts}.")
        # remaining unprocessed parts are taken to be complete
        if len(unprocessed_parts) >= 1:
            STATE_part_to_process = unprocessed_parts[0]
    else:
        # not recording and procesed everything
        STATE_part_to_process = None

    STATE_hpkv_cache = STATE_hpkv.get_cache()

    if STATE_part_to_process is not None:
        STATE_processed_parts.append(STATE_part_to_process)

        return [STATE_part_to_process]
    else:
        return None


def setupstage(stage, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hpkv_cache
    context_obj = None
    if hasattr(stage, "CONTEXT"):
        context_obj = stage.CONTEXT
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
    global STATE_notes, STATE_env, STATE_hpkv, STATE_hpkv_cache, STATE_part_to_process

    if "POSTPROC_PROGRESS_REDIS_CHANNEL" not in STATE_env:
        return

    redis_channel = STATE_env["POSTPROC_PROGRESS_REDIS_CHANNEL"]
    redis_obj = STATE_hpkv.redis_obj

    progress_statement = {
        "hostname": STATE_hpkv.hostname,
        "instance_id": STATE_hpkv.instance_id,
        "observation_id": STATE_hpkv_cache.get("OBSID"),
        "process_id": str(kwargs["process_id"]),
        "context_outputs": [STATE_part_to_process],
    }
    try:
        progress_statement["process_note"] = ProcessNote.string(processnote)
    except:
        progress_statement["process_note"] = "Unknown"

    if processnote == ProcessNote.Start:
        STATE_notes["start"] = time.time()
        STATE_notes["stages"] = {}
    elif processnote == ProcessNote.StageStart:
        stage_name = kwargs["stage"].NAME
        STATE_notes["stages"][stage_name] = {
            "start": time.time()
        }

        progress_statement["stage_name"] = stage_name
        progress_statement["stage_inputs"] = kwargs["inpvalue"]
        progress_statement["stage_arguments"] = kwargs["argvalue"]
        progress_statement["stage_environment"] = kwargs["envvalue"]
    elif processnote == ProcessNote.StageFinish:
        stage_name = kwargs["stage"].NAME
        time_now = time.time()
        STATE_notes["stages"][stage_name]["finish"] = time_now
        stage_duration = time_now - STATE_notes["stages"][stage_name]["start"]
        
        kwargs["logger"].info(f"Stage '{stage_name}' finished after {stage_duration}")

        progress_statement["stage_name"] = stage_name
        progress_statement["stage_output"] = kwargs["output"]
    elif processnote == ProcessNote.StageError:
        stage_name = kwargs["stage"].NAME
        time_now = time.time()
        STATE_notes["stages"][stage_name]["error"] = time.time()
        stage_duration = time_now - STATE_notes["stages"][stage_name]["start"]
        
        kwargs["logger"].info(f"Stage '{stage_name}' errored after {stage_duration}")

        progress_statement["stage_name"] = stage_name
        progress_statement["error"] = repr(kwargs["error"])
        progress_statement["traceback"] = traceback.format_exc()
    elif processnote == ProcessNote.Finish:
        STATE_notes["finish"] = time.time()
        kwargs["logger"].info(_get_notes_summary(STATE_notes))
    elif processnote == ProcessNote.Error:
        STATE_notes["error"] = time.time()
        logger = kwargs["logger"]

        progress_statement["error"] = repr(kwargs["error"])
        progress_statement["traceback"] = traceback.format_exc()

        if STATE_env.get("POSTPROC_REMOVE_FAILURES", "true").lower() != "false":
            try:
                os.remove(STATE_part_to_process)
                logger.warning(f"Process failed. Removed {STATE_part_to_process}.")
            except:
                logger.warning(f"Process failed but could not remove {STATE_part_to_process} ({traceback.format_exc()}).")
        else:
            logger.warning(f"Not removing {STATE_part_to_process}.")

        logger.info(_get_notes_summary(STATE_notes))
        

    kwargs["logger"].debug(progress_statement)
    redis_obj.publish(
        redis_channel,
        json.dumps(progress_statement)
    )


def _get_notes_summary(notes: dict):
    summary = "No stages to summarise..."
    if "stages" in notes:
        summary = "Summary:"
        for stage, stage_times in notes["stages"].items():
            tA, tB = stage_times.values()
            summary += f"\n\t{stage}: {abs(tB-tA):0.2f} s"
            if "error" in stage_times.keys():
                summary += " (Errored)"

    if "start" in notes:
        duration = - notes["start"]
        if "error" in notes:
            duration += notes["error"]
        elif "finish" in notes:
            duration += notes["finish"]
        else:
            duration += time.time()
        summary += f"\nTotal elapsed: {duration:0.2f} s"
    return summary


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
