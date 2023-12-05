import os
import glob
import redis
import logging
import json

from Pypeline import ProcessNote

import common

NAME = "pypeline_status_watcher"

STATE_observation_message = None
STATE_redis = None
STATE_redis_pubsub = None
STATE_env = {}

def setup(hostname, instance, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_redis, STATE_redis_pubsub

    STATE_redis = redis.Redis(host="redishost", decode_responses=True)
    STATE_redis_pubsub = STATE_redis.pubsub(
        ignore_subscribe_messages = True
    )
    STATE_redis_pubsub.subscribe("observations")


def dehydrate():
    global STATE_observation_message
    return {
        "observation_message": STATE_observation_message,
    }


def rehydrate(dehydration_dict):
    global STATE_observation_message
    
    STATE_observation_message = dehydration_dict["observation_message"]


def run(env=None, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    global STATE_redis_pubsub, STATE_observation_message, STATE_env

    STATE_env.update(common.env_str_to_dict(env))
    timeout_s = int(STATE_env.get("GET_OBS_MESSAGE_TIMEOUT", -1))

    message = STATE_redis_pubsub.get_message(timeout=timeout_s if timeout_s >= 0 else None)
    if message is None:
        logger.info("No message on 'observations'")
        return None
    try:
        STATE_observation_message = json.loads(message["data"])
        
        logger.info(f"Received: {STATE_observation_message}")
    except BaseException as err:
        raise ValueError(f"Could not parse {message}") from err
    
    return [STATE_observation_message.copy()]


def setupstage(stage, logger = None):
    pass


def note(processnote: ProcessNote, **kwargs):
    pass

