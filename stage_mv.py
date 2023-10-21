import subprocess
import glob
import os
import argparse
import logging
import shutil
from common import makedirs

from Pypeline import replace_keywords

ENV_KEY = None
ARG_KEY = "MoveARG"
INP_KEY = "MoveINP"
NAME = "mv"

CONTEXT = {
    "PROJID": None,
    "OBSID": None,
    "DATASET": None,
}

def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) == 0:
        logger.error("mv requires at least one input to move..")
        return []
    
    parser = argparse.ArgumentParser(
        description="A module to move input files to a destination.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "destination_dirpath",
        type=str,
        help="The destination directory.",
    )
    parser.add_argument(
        "--copy",
        action="store_true",
        help="Copy instead of move.",
    )
    if argstr is None:
        argstr = ""
    argstr = replace_keywords(CONTEXT, argstr)
    arglist = [arg for arg in argstr.split(" ") if len(arg) != 0]
    args = parser.parse_args(arglist)

    if not os.path.exists(args.destination_dirpath):
        logger.info(f"Creating destination directory: {args.destination_dirpath}")
        makedirs(args.destination_dirpath, user="cosmic", group="cosmic", mode=0o777, exist_ok=True)

    all_copied = []
    for inputpath in inputs:
        filename = os.path.basename(inputpath)
        destinationpath = os.path.join(args.destination_dirpath, filename)
        cmd = [
            "mv" if not args.copy else "cp",
            inputpath,
            destinationpath
        ]
        logger.info(" ".join(cmd))
        output = subprocess.run(cmd, capture_output=True)
        if output.returncode != 0:
            raise RuntimeError(output.stderr.decode())
        shutil.chown(destinationpath, user="cosmic", group="cosmic")
            
        all_copied.append(destinationpath)

    return all_copied
