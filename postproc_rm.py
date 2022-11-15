import subprocess
import glob
import os
import argparse

PROC_ENV_KEY = None
PROC_ARG_KEY = "REMVARG"
PROC_INP_KEY = "REMVINP"
PROC_NAME = "rm"


def run(argstr, inputs, env):
    if len(inputs) == 0:
        print("rm requires at least one input. It deletes files matching `glob.glob(input) for input in inputs`.")
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
    args = parser.parse_args(argstr.split(" "))

    all_deleted = []
    for inputpath in inputs:
        matchedfiles = glob.glob(f"{inputpath}{args.suffix}")
        for m in matchedfiles:
            cmd = ["rm", "-rf", m]
            print(" ".join(cmd))
            output = subprocess.run(cmd, capture_output=True)
            if output.returncode != 0:
                raise RuntimeError(output.stderr.decode())
            
        all_deleted.extend(matchedfiles)

    if args.dir_if_empty:
        common_dir = os.path.commonpath(all_deleted)
        if os.path.isfile(common_dir):
            common_dir = os.path.dirname(common_dir)
        contents = os.listdir(common_dir)
        if len(contents) == 0:
            os.removedir(common_dir)
            print(f"Removed empty-directory: {common_dir}")
        else:
            print(f"Common directory {common_dir} is not empty: {contents}")

    return all_deleted

if __name__ == "__main__":
    print(
        run(
            "",
            ["/mnt/buf0/discard/GUPPI/guppi_59897_51574_42458152839_AGC181474_0001.0000.raw"],
            None
        )
    )
