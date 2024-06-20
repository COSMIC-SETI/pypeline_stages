#!/usr/bin/env python
import logging, os, argparse, json, glob
import common

from blri.entrypoints import pycorr

ENV_KEY = None
ARG_KEY = "BlriPycorrARG"
INP_KEY = "BlriPycorrINP"
NAME = "blri_pycorr"


def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) != 1:
        raw_filepaths = list(
            filter(
                lambda s: s.endswith(".raw"),
                inputs
            )
        )
        if len(raw_filepaths) != len(inputs):
            logger.warning(f"{NAME} only takes RAW filepaths, filtering.")
        inputs = raw_filepaths

    
    bfr5genie.logger.handlers.clear()
    for handler in logger.handlers:
        bfr5genie.logger.addHandler(handler)

    arg_values = common.split_argument_string(argstr)

    logger.debug(f"arg_values: {arg_values}")

    return [pycorr.main(arg_values + inputs)]

if __name__ == "__main__":
    import sys
    logger = logging.getLogger(NAME)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    args = [
        "-t", "/home/cosmic/src/telinfo_vla.toml",
        "-u", 4,
        "-i", 16,
        "/mnt/buf0/test/GUPPI/20A-346.sb43317053.eb43427915.59964.7706725926.70.1_AC_8BIT.0000.raw"
    ]
    
    if len(sys.argv) > 1:
        args = sys.argv[1:]
    else:
        logger.warning(f"Using default arguments: {args}")

    print(
        run(
            " ".join(args[0:-1]),
            args[-1:],
            None
        )
    )
