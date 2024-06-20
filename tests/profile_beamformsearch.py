import os, logging, argparse, subprocess, time, socket
import glob
import multiprocessing as mp
from typing import List

import h5py

from Pypeline import import_module, process, ProcessParameters
from Pypeline.log_formatter import LogFormatter
from Pypeline.identifier import Identifier

mp.set_start_method("fork")

def _query_device(logger, gpu_id, attributes: List[str]):
    nvidia_query_cmd = f"nvidia-smi -i {gpu_id} --query-gpu={','.join(attributes)} --format=csv"
    output = subprocess.run(nvidia_query_cmd.split(" "), capture_output=True)
    if output.returncode != 0:
        logger.error(f"Device query failed: {output.stdout.decode()}")
        return {}
    else:
        output_lines = output.stdout.decode().strip().split("\n")
        logger.debug(f"{output_lines}")
        return dict(zip(
            attributes,
            output_lines[1].split(", ")
        ))

parser = argparse.ArgumentParser(
    description="process Beamform-Search post-process",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

parser.add_argument(
    "rawfile", type=str, help="The rawfile to use."
)

parser.add_argument(
    "bfr5file", type=str, help="The bfr5 file to use, or the stem of files to iterate through."
)

parser.add_argument(
    "--channelization-rates", type=int, nargs="+",
    default=[131072, 262144, 524288, 1048576],
    help="The channelization rates to assess performance under."
)

parser.add_argument(
    "--beamformer-rates", type=int, nargs="+",
    default=[1, 2, 4, 8, 16, 32, 64],
    help="The beamformer rates to assess performance under."
)

parser.add_argument(
    "--instance-id", type=int,
    default=4
)

parser.add_argument(
    "--gpu-id", type=int,
    default=0
)


args = parser.parse_args()

redis_kvcache = {
    "#CONTEXT": "shell",
    "#STAGES": "beamform_search",
    "BeamformSearchENV": "PATH=$PATH:/home/cosmic/src/blade/install/bin LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/cosmic/src/blade/install/lib/:/home/cosmic/src/blade/install/lib/x86_64-linux-gnu",
    "BeamformSearchARG": None,
    "BeamformSearchINP": "*shell",
}

context_name = redis_kvcache["#CONTEXT"]

context_dict = {}
assert import_module(context_name, modulePrefix="context", definition_dict=context_dict)
context = context_dict.pop(context_name)

instance_hostname = socket.gethostname()
instance_id = args.instance_id

logger = logging.getLogger(f"{instance_hostname}:{instance_id}")
ch = logging.StreamHandler()
ch.setFormatter(LogFormatter())
logger.setLevel(logging.INFO)
logger.addHandler(ch)

logging.getLogger(f"{instance_hostname}:{instance_id}.0").setLevel(logging.DEBUG)


bfr5filepaths = [args.bfr5file]
if not os.path.exists(args.bfr5file):
    bfr5filepaths = glob.glob(f"{args.bfr5file}*.bfr5")
    assert len(bfr5filepaths) > 0
    logger.info(f"Will iterate through these BFR5 files: {bfr5filepaths}")

assessments = []

csv_filepath = "blade_profiles.csv"
csv_headers = ["chanrate", "bfrate", "elapsed_s", "number_of_beams", "gpu_memuse_MiB", "successful"]
with open(csv_filepath, "w") as fio:
    fio.write(f"{','.join(csv_headers)}\n")

for bfr5_filepath in bfr5filepaths:
    bfr5 = h5py.File(bfr5_filepath, 'r')
    number_of_beams = len(list(bfr5["beaminfo"]["ras"][:]))

    for chanrate in args.channelization_rates:
        for bfrate in args.beamformer_rates:
            # redis_kvcache["BeamformSearchARG"] = f"-c {chanrate} -C 1 -T {bfrate} -N 1 --gpu-id {args.gpu_id} --snr-threshold 8.0 --drift-rate-zero-excluded --drift-rate-maximum 50.0"
            redis_kvcache["BeamformSearchARG"] = f"-c {chanrate} -C 1 -T {bfrate} -N 1 --gpu-id {args.gpu_id} --snr-threshold 8.0 --drift-rate-maximum 50.0"

            assessment = {
                "chanrate": chanrate,
                "bfrate": bfrate,
                "elapsed_s": -1,
                "number_of_beams": number_of_beams,
                "gpu_memuse_MiB": -1
            }

            device_prior_memuse_MiB = _query_device(logger, args.gpu_id, ["memory.used"])["memory.used"]
            assert device_prior_memuse_MiB[-4:] == " MiB"
            device_prior_memuse_MiB = int(device_prior_memuse_MiB[:-4])

            logger.info(f"Starting: c {chanrate} -T {bfrate}")

            with mp.Pool(processes=1) as pool:
                start = time.time()
                async_process = pool.apply_async(
                    process,
                    (
                        Identifier(instance_hostname, instance_id, 0),
                        ProcessParameters(
                            redis_kvcache,
                            {
                                context_name: [args.rawfile, bfr5_filepath],
                            },
                            redis_kvcache["#STAGES"].split(" "),
                            context.dehydrate(),
                            "redishost",
                            6379,
                        )
                    )
                )

                assessment["gpu_memuse_MiB"] = 0
                while not async_process.ready():
                    device_memuse_MiB = _query_device(logger, args.gpu_id, ["memory.used"]).get("memory.used", "-1 MiB")
                    device_memuse_MiB = int(device_memuse_MiB[:-4])
                    if device_memuse_MiB > assessment["gpu_memuse_MiB"]:
                        assessment["gpu_memuse_MiB"] = device_memuse_MiB

                    time.sleep(1)

                assessment["gpu_memuse_MiB"] -= device_prior_memuse_MiB
                assessment["successful"] = async_process.successful()
                assessment["elapsed_s"] = time.time() - start

                try:
                    async_process.get()
                except BaseException as err:
                    logger.error(f"Failed: {err}")

            with open(csv_filepath, "a") as fio:
                fio.write(','.join(str(assessment[hdr]) for hdr in csv_headers))
                fio.write("\n")

            assessments.append(assessment)
            logger.info(f"{assessments}")
