import os, time, traceback
import shutil
from typing import Dict

from Pypeline import ProcessNote


def env_str_to_dict(env_value):
    env_dict = {}
    if env_value is None:
        return env_dict

    for variablevalues in env_value.split(" "):
        if "=" in variablevalues:
            pair = variablevalues.split("=")
            env_dict[pair[0]] = pair[1]
    return env_dict

def directory_exists(dirpath):
    return os.path.exists(dirpath) and os.path.isdir(dirpath)

def makedirs(dirpath, user=None, group=None, mode=None, exist_ok=False):
    if directory_exists(dirpath):
        if not exist_ok:
            raise FileExistsError(dirpath)
        return

    sub_directories_to_make = []
    existing_root = dirpath
    while True:
        existing_root, subdir = os.path.split(existing_root)
        sub_directories_to_make.insert(0, subdir)
        if directory_exists(existing_root):
            break

    try:
        os.makedirs(dirpath, exist_ok=False)
    except FileExistsError as err:
        # directories are made since last looking
        if not exist_ok:
            raise err
        return

    for subdir in sub_directories_to_make:
        existing_root = os.path.join(existing_root, subdir)

        if user is not None or group is not None:
            shutil.chown(existing_root, user=user, group=group)
        if mode is not None:
            os.chmod(existing_root, mode=mode)


def context_build_statement_of_note(progress_statement: Dict, processnote: ProcessNote, kwargs: Dict):
    try:
        progress_statement["process_note"] = ProcessNote.string(processnote)
    except:
        progress_statement["process_note"] = "Unknown"

    if processnote == ProcessNote.Start:
        pass
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
    elif processnote == ProcessNote.Error:
        progress_statement["error"] = repr(kwargs["error"])
        progress_statement["traceback"] = traceback.format_exc()


def context_take_note(state_dict, processnote: ProcessNote, kwargs: Dict):
    logger = kwargs["logger"]
    time_now = time.time()

    if processnote == ProcessNote.Start:
        state_dict["start"] = time_now
        state_dict["stages"] = {}

    elif processnote == ProcessNote.StageStart:
        stage_name = kwargs["stage"].NAME
        state_dict["stages"][stage_name] = {
            "start": time_now
        }

    elif processnote == ProcessNote.StageFinish:
        stage_name = kwargs["stage"].NAME
        time_now = time_now
        state_dict["stages"][stage_name]["finish"] = time_now
        stage_duration = time_now - state_dict["stages"][stage_name]["start"]
        
        logger.info(f"Stage '{stage_name}' finished after {stage_duration}")

    elif processnote == ProcessNote.StageError:
        stage_name = kwargs["stage"].NAME
        time_now = time_now
        state_dict["stages"][stage_name]["error"] = time_now
        stage_duration = time_now - state_dict["stages"][stage_name]["start"]
        
        logger.info(f"Stage '{stage_name}' errored after {stage_duration}")

    elif processnote == ProcessNote.Finish:
        state_dict["finish"] = time_now
        logger.info(_get_notes_summary(state_dict))

    elif processnote == ProcessNote.Error:
        state_dict["error"] = time_now
        logger.info(_get_notes_summary(state_dict))


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
