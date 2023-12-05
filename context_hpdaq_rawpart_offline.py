import os
import glob
import re
import json
import logging
import traceback
import time
import subprocess
import shutil

import redis

from Pypeline import ProcessNote

from guppi import GuppiRawHandler

import common

NAME = "hpdaq_rawpart_offline"

STATE_notes = {}
STATE_env = {}
STATE_hostname_instance_tuple = None
STATE_batch_iter = None
STATE_current_guppi0_header = None
STATE_current_batch = None


def setup(hostname, instance, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_hostname_instance_tuple
    STATE_hostname_instance_tuple = (hostname, instance)

def dehydrate():
    global STATE_notes, STATE_env, STATE_current_batch, STATE_current_guppi0_header
    return {
        "notes": STATE_notes,
        "hostname_instance_tuple": STATE_hostname_instance_tuple,
        "env": STATE_env, 
        "current_batch": STATE_current_batch,
        "current_guppi0_header": STATE_current_guppi0_header,
    }


def rehydrate(dehydration_dict):
    global STATE_notes, STATE_env, STATE_hostname_instance_tuple, STATE_current_guppi0_header

    STATE_notes = dehydration_dict["notes"]
    STATE_env = dehydration_dict["env"]
    STATE_hostname_instance_tuple = dehydration_dict["hostname_instance_tuple"]
    STATE_current_batch = dehydration_dict["current_batch"]
    STATE_current_guppi0_header = dehydration_dict["current_guppi0_header"]


def _process_raw_filepath(filepath, logger=None):
    # _, filename = os.path.split(filepath)
    m = re.match(
        r"(?P<directory>.*)/(?P<obsid>.*)\.(?P<enumeration>\d{4})\.raw",
        filepath
    )
    if m is None and logger is not None:
        logger.warning(f"Could not match '{filepath}' to `(.*)/(.*)\.(\d{4})\.raw`")
    return m


def run(env=None, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    
    global STATE_env, STATE_batch_iter, STATE_current_guppi0_header, STATE_current_batch

    if STATE_batch_iter is None:
        # initial setup
        STATE_env.clear()
        STATE_env.update(common.env_str_to_dict(env))
        logger.info(f"{STATE_env}")
        rawpart_groups = {}

        rawpart_filepaths = glob.glob(STATE_env["RAWPART_GLOB_PATTERN"])
        logger.debug(f"len(rawpart_filepaths): {len(rawpart_filepaths)}")
        rawpart_filepaths.sort()
        for rawpart_filepath in rawpart_filepaths:
            regex_match = _process_raw_filepath(rawpart_filepath, logger=logger)

            obsid_parts = rawpart_groups.get(regex_match.group("obsid"), [])
            obsid_parts.append(rawpart_filepath)
            rawpart_groups[regex_match.group("obsid")] = obsid_parts
        
        batch_length = max(
            int(STATE_env.get("BATCH_RAWPART_COUNT", 1)),
            1
        )
        batches = []
        for obsid, obs_rawpart_filepaths in rawpart_groups.items():
            obs_rawpart_filepaths.sort()
            prev_enumeration = None
            enumeration = None
            batch = []

            while len(obs_rawpart_filepaths) > 0:
                if len(batch) == batch_length:
                    batches.append(batch)
                    batch = []

                filepath = obs_rawpart_filepaths.pop(0)
                regex_match = _process_raw_filepath(filepath, logger=logger)
                
                prev_enumeration = enumeration
                enumeration = int(regex_match.group("enumeration"))
                if prev_enumeration is None:
                    batch.append(filepath)
                    continue
                
                if (prev_enumeration + 1) == enumeration:
                    batch.append(filepath)
                    continue

                # filepath is not subsequent so push onto next batch
                batches.append(batch)
                batch = [filepath]
            if len(batch) == batch_length:
                batches.append(batch)
        
        STATE_batch_iter = iter(batches)

    try:
        STATE_current_batch = next(STATE_batch_iter)
        STATE_current_guppi0_header = {}
        if len(STATE_current_batch) > 0:
            for hdr, data in GuppiRawHandler(STATE_current_batch[0]).blocks():
                STATE_current_guppi0_header.update(hdr)
                break
        
        # provide common property request as a key-value in case the property isn't
        # set on the class instance...
        STATE_current_guppi0_header["project_id"] = STATE_current_guppi0_header.get(
            "PROJID",
            None
        )
        return STATE_current_batch
    except StopIteration:
        return False


def setupstage(stage, logger = None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_current_guppi0_header, STATE_current_batch
    context_obj = None
    if hasattr(stage, "CONTEXT"):
        context_obj = stage.CONTEXT
    else:
        logger.debug(f"Stage has no CONTEXT to populate.")
        return

    for key in context_obj.keys():
        try:
            context_obj[key] = (
                getattr(STATE_current_guppi0_header, key)
                if hasattr(STATE_current_guppi0_header, key)
                else STATE_current_guppi0_header[key]
            )
        except:
            logger.error(f"Could not populate key: {key}.")


def note(processnote: ProcessNote, **kwargs):
    global STATE_notes, STATE_env, STATE_hostname_instance_tuple, STATE_current_guppi0_header, STATE_current_batch

    common.context_take_note(STATE_notes, processnote, kwargs)
    logger = kwargs["logger"]

    if processnote == ProcessNote.Error:
        if STATE_env.get("POSTPROC_MOVE_FAILURES", "false").lower() != "false":
            for part_to_process in STATE_current_batch:
                filename = os.path.basename(part_to_process)
                destinationpath = os.path.join("/srv/data0/batch_processed_vlass_problematic/", filename)
                cmd = [
                    "mv",
                    part_to_process,
                    destinationpath
                ]
                logger.info(" ".join(cmd))
                output = subprocess.run(cmd, capture_output=True)
                if output.returncode != 0:
                    # raise RuntimeError(output.stderr.decode())
                    logger.error(f"Process failed but could not move {part_to_process} ({traceback.format_exc()}).")
                else:
                    shutil.chown(destinationpath, user="cosmic", group="cosmic")
                    logger.warning(f"Process failed. Moved {part_to_process}.")
                    
        else:
            logger.warning(f"Not moving {STATE_current_batch}.")


if __name__ == "__main__":
    import socket

    setup(socket.gethostname(), 1)
    print(run())
