import logging

from Pypeline import import_stage, process
from Pypeline.log_formatter import LogFormatter
from Pypeline.identifier import Identifier

redis_cache = {
    "#PRIMARY": "hpdaq_rawpart",
    "#STAGES": "bfr5_generate beamform_search",
    "BFR5GenerateARG": "--telescope-info-toml-filepath /home/cosmic/src/telinfo_vla.toml --targets-redis-key-prefix targets:MeerKAT-example:array_1 --targets-redis-key-timestamp 20230111T234728Z --take-targets 5",
    "BFR5GenerateINP": "hpdaq_rawpart",
    "BeamformSearchENV": "PATH=$PATH:/home/cosmic/src/blade/install/bin LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/cosmic/src/blade/install/lib/:/home/cosmic/src/blade/install/lib/x86_64-linux-gnu",
    "BeamformSearchARG": "-c 131072 -C 1 -T 64 -N 1 --gpu-shares 2 --gpu-share-index $inst$ --gpu-target-most-memory",
    "BeamformSearchINP": "hpdaq_rawpart bfr5_generate",
}

initial_stage_name = redis_cache["#PRIMARY"]

initial_stage_dict = {}
assert import_stage(initial_stage_name, stagePrefix="proc", definition_dict=initial_stage_dict)
initial_stage = initial_stage_dict.pop(initial_stage_name)

instance_hostname = "cosmic-gpu-0"
instance_id = 0

logger = logging.getLogger(f"{instance_hostname}:{instance_id}")
ch = logging.StreamHandler()
ch.setFormatter(LogFormatter())
logger.addHandler(ch)
logger.setLevel(logging.DEBUG)

initial_stage.setup(
    instance_hostname,
    instance_id,
    logger=logger
)

proc_outputs = initial_stage.run(logger=logger)
print(f"proc_outputs: {proc_outputs}")

process(
    Identifier(instance_hostname, instance_id, 0),
    redis_cache,
    {
        initial_stage_name: ["/mnt/buf0/faux_targets_bf_search/GUPPI/23A-345.sb43055931.eb43582535.59974.65680560185.1.1_AC_8BIT.0000.raw"],
    },
    initial_stage.dehydrate(),
    "redishost",
    6379,
)