import logging

from Pypeline import import_module, process, ProcessParameters
from Pypeline.log_formatter import LogFormatter
from Pypeline.identifier import Identifier

redis_kvcache = {
    "#CONTEXT": "hpdaq_rawpart",
    "#STAGES": "bfr5_generate beamform_search",
    "BFR5GenerateARG": "--telescope-info-toml-filepath /home/cosmic/src/telinfo_vla.toml --targets-redis-key-prefix targets:MeerKAT-example:array_1 --targets-redis-key-timestamp 20230111T234728Z --take-targets 5",
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

context_outputs = context.run(logger=logger)
print(f"context_outputs: {context_outputs}")

process(
    Identifier(instance_hostname, instance_id, 0),
    ProcessParameters(
        redis_kvcache,
        {
            context_name: ["/mnt/buf0/faux_targets_bf_search/GUPPI/23A-345.sb43055931.eb43582535.59974.65680560185.1.1_AC_8BIT.0000.raw"],
        },
        redis_kvcache["#STAGES"].split(" "),
        context.dehydrate(),
        "redishost",
        6379,
    )
)