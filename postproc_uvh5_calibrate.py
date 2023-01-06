import subprocess
import os
import argparse

PROC_ENV_KEY = "UVH5CalibrateENV"
PROC_ARG_KEY = "UVH5CalibrateARG"
PROC_INP_KEY = "UVH5CalibrateINP"
PROC_NAME = "uvh5_calibrate"

def run(argstr, inputs, env):
    if len(inputs) != 1:
        print("calibrate_uvh5 requires one input, the uvh5 filepath.")
        return None
    
    # parser = argparse.ArgumentParser(
    #     description="A wrapper for the calibrate_uvh5 script.",
    #     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    # )
    # parser.add_argument(
    #     "-o",
    #     type=str,
    #     default='',
    #     help="The output log directory.",
    # )
    # args = parser.parse_args(argstr.split(" "))

    cmd = f"python3 calibrate_uvh5.py -d {inputs[0]} {argstr}"

    env_base = os.environ.copy()
    if env is not None:
        for variablevalues in env.split(" "):
            if ":" in variablevalues:
                pair = variablevalues.split(":")
                env_base[pair[0]] = pair[1]

    print(cmd)
    output = subprocess.run(
        cmd,
        env=env_base,
        capture_output=True,
        shell=True,
        cwd="/home/cosmic/dev/COSMIC-VLA-CalibrationEngine/"
    )
    if output.returncode != 0:
        raise RuntimeError(output.stderr.decode())
    output = output.stdout.decode().strip()
    print(output)
    
    return []

if __name__ == "__main__":
    print(
        run(
            "-o ./ --genphase --pub-to-redis",
            ["/mnt/buf0/uvh5_commensal/uvh5/uvh5_59949_05980_51052330932_J0303+4716_0001.uvh5"],
            "CONDA_PYTHON_EXE:/home/svarghes/anaconda3/envs/csomic_cal/bin/python3"
        )
    )
