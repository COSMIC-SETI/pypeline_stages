import logging, argparse, subprocess, time

from Pypeline import import_module, process, ProcessParameters
from Pypeline.log_formatter import LogFormatter
from Pypeline.identifier import Identifier


parser = argparse.ArgumentParser(
    description="Assess Beamform-Search post-process",
    formatter_class=argparse.ArgumentDefaultsHelpFormatter,
)

parser.add_argument(
    "rawfile", type=str, help="The rawfile to use."
)

parser.add_argument(
    "--power-limits", type=int, nargs="+", default=[100, 110, 120, 130, 140], help="The power limits to assess performance under."
)

args = parser.parse_args()

redis_kvcache = {
    "#CONTEXT": "hpdaq_rawpart",
    "#STAGES": "bfr5_generate beamform_search",
    "BFR5GenerateARG": "--telescope-info-toml-filepath /home/cosmic/src/telinfo_vla.toml --take-targets 5 --targets-redis-key-timestamp 0 --targets-redis-key-prefix targets:VLA-COSMIC:test_array",
    "BFR5GenerateINP": "hpdaq_rawpart",
    "BeamformSearchENV": "PATH=$PATH:/home/cosmic/src/blade/install/bin LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/cosmic/src/blade/install/lib/:/home/cosmic/src/blade/install/lib/x86_64-linux-gnu",
    "BeamformSearchARG": "-c 131072 -C 1 -T 64 -N 1 --gpu-shares 2 --gpu-share-index $inst$ --gpu-target-most-memory",
    "BeamformSearchINP": "hpdaq_rawpart bfr5_generate",
}

context_name = redis_kvcache["#CONTEXT"]

context_dict = {}
assert import_module(context_name, modulePrefix="context", definition_dict=context_dict)
context = context_dict.pop(context_name)

instance_hostname = "cosmic-gpu-0"
instance_id = 0

logger = logging.getLogger(f"{instance_hostname}:{instance_id}")
ch = logging.StreamHandler()
ch.setFormatter(LogFormatter())
logger.addHandler(ch)
logger.setLevel(logging.INFO)

context.setup(
    instance_hostname,
    instance_id,
    logger=logger
)

assessments = []

for power_limit in args.power_limits:
    assessment = {}

    power_throttle_cmd = f"nvidia-smi -pl {power_limit}".split(" ")
    assessment["power_throttle_cmd"] = power_throttle_cmd

    output = subprocess.run(power_throttle_cmd, capture_output=True)
    if output.returncode != 0:
        assessment["power_throttle_success"] = False
        assessment["power_throttle_error"] = output.stderr.decode()
        assessments.append(assessment)
        continue
    
    assessment["power_throttle_success"] = True

    start = time.time()
    process(
        Identifier(instance_hostname, instance_id, 0),
        ProcessParameters(
            redis_kvcache,
            {
                context_name: [args.rawfile],
            },
            redis_kvcache["#STAGES"].split(" "),
            context.dehydrate(),
            "redishost",
            6379,
        )
    )
    elapsed = time.time() - start
    assessment["beamformsearch_elapsed_s"] = elapsed
    assessments.append(assessment)

logger.info(f"{assessments}")
