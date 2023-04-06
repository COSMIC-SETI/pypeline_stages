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
    
    arg_strs = argstr.split('"')

    arg_values = []
    for argstr_index, arg_str in enumerate(arg_strs):
        if argstr_index%2 == 1:
            # argstr was encapsulated in quotations
            arg_values.append(arg_str)
        elif len(arg_str.strip()) > 0:
            argstr_values = arg_str.strip().split(" ")
            if argstr_index == len(arg_strs)-1 and len(arg_strs)%2 == 0:
                argstr_values[0] = f'"{argstr_values[0]}'

            arg_values += argstr_values

    logger.debug(f"arg_values: {arg_values}")
    arg_values += inputs

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
        "--targets-redis-key-prefix", "targets:VLA-COSMIC:vlass_array",
        "--take-targets", "0",
        "--target", '"Voyager 1"',
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
