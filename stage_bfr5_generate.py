#!/usr/bin/env python
import logging, os, argparse, json, glob

from bfr5genie import entrypoints

ENV_KEY = None
ARG_KEY = "BFR5GenerateARG"
INP_KEY = "BFR5GenerateINP"
NAME = "bfr5_generate"


def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) != 1:
        logger.error("bfr5_generate requires one input, the RAW filepath.")
        return None
    
    arg_values = argstr.split(' ') + inputs

    if any(arg in arg_values for arg in ['--take-targets', '--target']):
        return [entrypoints.generate_targets_for_raw(arg_values)]
    elif any(arg in arg_values for arg in ['--raster-ra']):
        return [entrypoints.generate_raster_for_raw(arg_values)]
    else:
        return [entrypoints.generate_for_raw(arg_values)]

if __name__ == "__main__":
    import sys
    logger = logging.getLogger(NAME)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    args = [
        "--telescope-info-toml-filepath",
        "/home/cosmic/src/telinfo_vla.toml",
        "/mnt/buf0/mydonsol_blade/bladetest_vlass_32c_128k.0000.raw"
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
