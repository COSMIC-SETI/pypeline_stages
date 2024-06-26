import subprocess
import glob
import os
import argparse
import logging
import re

from Pypeline import replace_keywords

import common

ENV_KEY = "BeamformSearchENV"
ARG_KEY = "BeamformSearchARG"
INP_KEY = "BeamformSearchINP"
NAME = "beamform_search"

CONTEXT = {
    "project_id": None,
    "OBSID": None,
}

def _select_gpu_with_most_memory(logger, gpu_share_index, gpu_shares):
    nvidia_query_cmd = "nvidia-smi --query-gpu=index,name,pci.bus_id,driver_version,pstate,utilization.gpu,utilization.memory,memory.total,memory.free --format=csv"
    output = subprocess.run(nvidia_query_cmd.split(" "), capture_output=True)
    if output.returncode != 0:
        logger.error(output.stderr.decode())
    else:
        nvidia_devs = output.stdout.decode().strip().split("\n")[1:]
        logger.info("\n".join(nvidia_devs))
        gpu_per_share = len(nvidia_devs) // gpu_shares
        gpu_row_start = gpu_share_index*gpu_per_share
        logger.info(
            f"Only considering index {gpu_share_index} of {gpu_shares} shares ({gpu_per_share} GPUs per share): rows {gpu_row_start} to {gpu_row_start+gpu_per_share-1}"
        )
        nvidia_devs = nvidia_devs[gpu_row_start : gpu_row_start+gpu_per_share]

        details = nvidia_devs[0].split(", ")
        nvidia_id = details[0]
        nvidia_memfree = int(details[-1][:-4]) # curtail " MiB"
        for nvidia_dev in nvidia_devs[1:]:
            details = nvidia_dev.split(", ")
            memfree = int(details[-1][:-4]) # curtail " MiB"
            if memfree > nvidia_memfree:
                nvidia_id = details[0]
                nvidia_memfree = memfree
        
        logger.info(f"Selected device #{nvidia_id} with {nvidia_memfree} MiB free.")
    return nvidia_id

def _add_args(parser):
    parser.add_argument(
        "-c",
        "--channelization-rate",
        type=int,
        default=131072,
        help="The upchannelization rate (pre beamformer)",
    )
    parser.add_argument(
        "-T",
        "--beamform-time",
        type=int,
        default=16,
        help="The amount of upchannelized time to beamform at a time",
    )
    parser.add_argument(
        "-C",
        "--coarse-channel-ingest-rate",
        type=int,
        default=1,
        help="The number of coarse channels to process at a time",
    )
    parser.add_argument(
        "-s",
        "--snr-threshold",
        type=float,
        default=0.0,
        help="The SETI search SNR threshold",
    )
    parser.add_argument(
        "-z",
        "--drift-rate-zero-excluded",
        action="store_true",
        help="The SETI search exclude hits with drift rates of zero",
    )
    parser.add_argument(
        "-d",
        "--drift-rate-minimum",
        type=float,
        default=0.0,
        help="The SETI search drift rate minimum",
    )
    parser.add_argument(
        "-D",
        "--drift-rate-maximum",
        type=float,
        default=50.0,
        help="The SETI search drift rate maximum",
    )
    parser.add_argument(
        "-N",
        "--number-of-workers",
        type=int,
        default=1,
        help="The number of parallel workers to execute with",
    )
    parser.add_argument(
        "-I",
        "--incoherent-beam",
        action="store_true",
        help="Form the incoherent beams.",
    )
    parser.add_argument(
        "-gs",
        "--gpu-shares",
        type=int,
        default=1,
        help="How many ways the available GPUs must be shared.",
    )
    parser.add_argument(
        "-gi",
        "--gpu-share-index",
        type=int,
        default=0,
        help="Which index of the shared GPUs must be used.",
    )
    parser.add_argument(
        "-gid",
        "--gpu-id",
        type=int,
        default=None,
        help="GPU device ID selection.",
    )
    parser.add_argument(
        "-gt",
        "--gpu-target-most-memory",
        action="store_true",
        help="Target the GPU with the most free memory.",
    )
    parser.add_argument(
        "-pl",
        "--gpu-power-limit",
        type=int,
        choices=range(100,141),
        default=100,
        help="Target the GPU with the most free memory.",
    )
    parser.add_argument(
        "-l",
        "--log-blade-output",
        action="store_true",
        help="Log the printout from BLADE in *.blade.stdout.txt.",
    )
    parser.add_argument(
        "--negate-phasor-delays",
        action="store_true",
        help="BLADE negates the delay values from which the phasors are calculated.",
    )
    parser.add_argument(
        "--mode-b",
        action="store_true",
        help="Employ Mode-B instead of Mode-BS.",
    )
    parser.add_argument(
        "-x",
        "--search-exclusion-subband",
        type=str,
        default=None,
        help="Exclusion subband CSV file.",
    )

def run(argstr, inputs, env, logger=None):
    global CONTEXT

    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) < 2:
        raise ValueError(f"{NAME} requires at least 2 inputs: the raw filepath and the BFR5 filepath.")
    
    rawfile_process_limit = 0 # unlimited

    # sort the input filepaths
    raw_filespaths = []
    bfr5_filespath = None
    raw_regex = r'(.*?)\.\d{4}\.raw'
    for inp in inputs:
        if re.match(raw_regex, inp):
            raw_filespaths.append(inp)
        elif inp.endswith(".bfr5"):
            if bfr5_filespath is not None:
                raise ValueError(f"{NAME} requires one BFR5 filepath and at least one RAW filepath. Too many BFR5 filepaths provided.")
            bfr5_filespath = inp
        else:
            raise ValueError(f"{NAME} requires one BFR5 filepath and at least one RAW filepath. Unrecognised input file type: '{inp}'.")
    
    raw_filespaths.sort()
    rawfile_process_limit = len(raw_filespaths)
    if rawfile_process_limit == 1:
        if not os.path.exists(raw_filespaths[0]):
            # stem provided, process all files
            rawfile_process_limit = 0

    inputs = [
        raw_filespaths[0],
        bfr5_filespath
    ]

    parser = argparse.ArgumentParser(
        description="A module to invoke BLADE Mode Beamform-SETISearch.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    _add_args(parser)
    parser.add_argument(
        "--output-stempath",
        type=str,
        default=inputs[0],
        help="The output directory of the products.",
    )
    argstr = replace_keywords(CONTEXT, argstr)
    args = parser.parse_args(argstr.split(" "))

    cmd = [
        "blade-cli",
        "-P", # disable progress-bar
        "--input-type", "CI8",
        "--output-type", "F32",
        "-t", "ATA",
        "-m", "BS" if not args.mode_b else "B",
        "--input-guppi-raw-limit", str(rawfile_process_limit)
    ]
    if args.drift_rate_zero_excluded:
        cmd.append('-Z')
    if args.incoherent_beam:
        cmd.append('-I')
    if args.search_exclusion_subband:
        cmd += ['-x', args.search_exclusion_subband]
    if args.negate_phasor_delays:
        cmd.append('--negate-phasor-delays')
    
    cmd.extend([
        "-s", str(args.snr_threshold),
        "-D", str(args.drift_rate_maximum),
        "-c", str(args.channelization_rate),
        "-T", str(args.beamform_time),
        "-C", str(args.coarse_channel_ingest_rate),
        "-N", str(args.number_of_workers),
        inputs[0],
        inputs[1],
        args.output_stempath
    ])

    logger.info(" ".join(cmd))

    env_base = os.environ.copy()
    env_base.update(common.env_str_to_dict(env))
    # logger.info(f"env: {env_base}")

    nvidia_id = args.gpu_id
    if args.gpu_target_most_memory:
        nvidia_id = _select_gpu_with_most_memory(
            logger,
            args.gpu_share_index,
            args.gpu_shares
        )

    if nvidia_id is not None:
        if "CUDA_VISIBLE_DEVICES" in env_base:
            logger.warning(f"Overriding CUDA_VISIBLE_DEVICES={env_base['CUDA_VISIBLE_DEVICES']}.")
        env_base["CUDA_VISIBLE_DEVICES"] = str(nvidia_id)
        logger.info(f"Set CUDA_VISIBLE_DEVICES={nvidia_id}.")

        # assume power limit set elsewhere
        # # power limit
        # powerlimit_cmd = [
        #     "nvidia-smi",
        #     "-pl", str(args.gpu_power_limit),
        # ]
        # powerlimit_cmd += [
        #     "-i", str(nvidia_id),
        # ]

        # logger.debug(f"{powerlimit_cmd}")
        # output = subprocess.run(powerlimit_cmd, capture_output=True)
        # if output.returncode != 0:
        #     logger.error(output.stdout.decode())

    logger.debug(f"{cmd}")
    output = subprocess.run(cmd, env=env_base, capture_output=True)
    stdoutput = output.stdout.decode().strip()
    stdoutput_last_line = stdoutput.split('\n')[-1]
    logger.info(f"Last stdout line: `{stdoutput_last_line}`")
    
    if output.returncode != 0:
        stderr_output = output.stderr.decode()
        if len(stderr_output) == 0:
            stderr_output = f"Nothing in stderr, possibly a more serious issue (segfault)."

        logger.error(stderr_output.strip())
        raise RuntimeError(stderr_output)

    outputs = glob.glob(f"{args.output_stempath}.seticore.*")
    outputs.extend(glob.glob(f"{args.output_stempath}-beam*.fil"))

    if args.log_blade_output:
        log_outputfilepath = f"{args.output_stempath}.blade.stdout.txt"
        with open(log_outputfilepath, "w") as fio:
            fio.write(stdoutput)
        outputs.append(log_outputfilepath)

    logger.info(f"Outputs: {outputs}")
    return outputs


if __name__ == "__main__":
    import sys
    logger = logging.getLogger(NAME)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    if len(sys.argv) <= 2:
        args = []
        inputs = [
            "/mnt/buf0/delay_modeling_comm_Bco/GUPPI/TCOS0001_S_3000.59963.98997351852.2.1_AC_8BIT.0000.raw",
            "~svarghes/benchmark_test/bfdata/bf_test.bfr5",
        ]
    elif len(sys.argv) > 3:
        args = sys.argv[1:-2]
        inputs = sys.argv[-2:]
    else:
        raise ValueError("Provide both a RAW filepath and a BFR5 filepath.")

    print(
        run(
            " ".join(args),
            inputs,
            None
        )
    )
