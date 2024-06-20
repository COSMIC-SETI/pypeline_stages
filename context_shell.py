import os
import glob
import redis
import logging

from Pypeline import ProcessNote

import common

NAME = "shell"


STATE_notes = {}


def setup(hostname, instance, logger=None):
    pass


def dehydrate():
    return {}


def rehydrate(dehydration_dict):
    pass


def run(env=None, logger=None):
    return []


def setupstage(stage, logger = None):
    pass


def note(processnote: ProcessNote, **kwargs):
    global STATE_notes

    common.context_take_note(STATE_notes, processnote, kwargs)

    progress_statement = {}
    common.context_build_statement_of_note(progress_statement, processnote, kwargs)
    kwargs["logger"].info(progress_statement)
