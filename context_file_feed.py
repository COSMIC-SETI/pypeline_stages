import logging

from Pypeline import ProcessNote

NAME = "file_feed"

STATE_outputs = None

def setup(hostname, instance, logger=None):
    return


def dehydrate():
    global STATE_outputs
    return (
        STATE_outputs,
    )


def rehydrate(dehydration_tuple):
    global STATE_outputs
    STATE_outputs = dehydration_tuple[0]


def run(env=None, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_outputs

    if env is None:
        return None
    
    if STATE_outputs is not None:
        return False

    STATE_outputs = env.split(" ")
    return STATE_outputs


def setupstage(stage, logger = None):
    return


def note(processnote: ProcessNote, **kwargs):
    return
