import logging
import subprocess
import os
import argparse
import time

import common

from Pypeline import replace_keywords

ENV_KEY = "UVH5CalibrateENV"
ARG_KEY = "UVH5CalibrateARG"
INP_KEY = "UVH5CalibrateINP"
NAME = "uvh5_calibrate"

CONTEXT = {
    "PROJID": None,
    "OBSID": None,
    "DATASET": None,
}

def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) != 1:
        logger.error(f"calibrate_uvh5 requires one input, the uvh5 filepath. Not: {inputs}")
        return None
    
    # parser = argparse.ArgumentParser(
    #     description="A wrapper for the calibrate_uvh5 script.",
    #     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    # )
    # parser.add_argument(
    #     "-o",
    #     type=str,
    #     default='',
    #     help="The output log directory.",
    # )
    # args = parser.parse_args(argstr.split(" "))

    argstr = replace_keywords(CONTEXT, argstr)
    cmd = f"/home/cosmic/anaconda3/envs/cosmic_vla/bin/python3 /home/cosmic/src/COSMIC-VLA-CalibrationEngine/calibrate_uvh5.py -d {inputs[0]} {argstr}"

    env_base = os.environ.copy()
    env_base.update(common.env_str_to_dict(env))

    t_start = time.time()
    logger.info(cmd)
    output = subprocess.run(
        cmd,
        env=env_base,
        capture_output=True,
        shell=True,
    )
    if output.returncode != 0:
        raise RuntimeError(output.stderr.decode())
    output = output.stdout.decode().strip()
    logger.info(output)

    t_elapsed = time.time() - t_start
    if t_elapsed > 8:
        raise RuntimeError(f"Took longer than 8 seconds: {t_elapsed} s")
    
    return []

if __name__ == "__main__":
    print(
        run(
            "-o ./ --genphase --pub-to-redis",
            ["/mnt/buf0/uvh5_commensal/uvh5/uvh5_59949_05980_51052330932_J0303+4716_0001.uvh5"],
            ""
        )
    )
