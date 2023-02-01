import subprocess
import glob
import os
import argparse
import logging

ENV_KEY = None
ARG_KEY = "RemovalARG"
INP_KEY = "RemovalINP"
NAME = "rm"

def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) == 0:
        logger.error("rm requires at least one input. It deletes files matching `glob.glob(input) for input in inputs`.")
        return []
    
    parser = argparse.ArgumentParser(
        description="A module to delete files based on an input.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "--suffix",
        type=str,
        default='',
        help="The suffix to the file pattern: `glob.glob({input}{suffix})`.",
    )
    parser.add_argument(
        "--dir-if-empty",
        action='store_true',
        help="Delete the deepest common directory of the inputs if it is empty after removal of inputs.",
    )
    if argstr is None:
        argstr = ""
    logger.info(f"Argument String: `{argstr}`")
    args = parser.parse_args(argstr.split(" "))

    all_deleted = []
    for inputpath in inputs:
        matchedfiles = glob.glob(f"{inputpath}{args.suffix}")
        for m in matchedfiles:
            cmd = ["rm", "-rf", m]
            logger.info(" ".join(cmd))
            output = subprocess.run(cmd, capture_output=True)
            if output.returncode != 0:
                raise RuntimeError(output.stderr.decode())
            
        all_deleted.extend(matchedfiles)

    if args.dir_if_empty:
        common_dir = os.path.commonpath(all_deleted)
        logger.debug(f"Common path for all deleted: {common_dir}")
        if os.path.isfile(common_dir):
            common_dir = os.path.dirname(common_dir)
            logger.debug(f"Common directory for all deleted: {common_dir}")

        if os.path.exists(common_dir):
            contents = os.listdir(common_dir)
            if len(contents) == 0:
                os.rmdir(common_dir)
                logger.info(f"Removed empty-directory: {common_dir}")
            else:
                logger.info(f"Common directory {common_dir} is not empty: {contents}")
        else:
            logger.warning(f"Common directory for all deleted does not exist: {common_dir}")

    return all_deleted

if __name__ == "__main__":
    print(
        run(
            "",
            ["/mnt/buf0/discard/GUPPI/guppi_59897_51574_42458152839_AGC181474_0001.0000.raw"],
            None
        )
    )
