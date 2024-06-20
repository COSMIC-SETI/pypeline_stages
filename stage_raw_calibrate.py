import subprocess
import os
import re
import logging

import common

from Pypeline import replace_keywords

ENV_KEY = "RawCalibrationENV"
ARG_KEY = "RawCalibrationARG"
INP_KEY = "RawCalibrationINP"
NAME = "raw_calibrate"

CONTEXT = {
    "PROJID": None,
    "OBSID": None,
    "DATASET": None,
}

def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) == 0:
        logging.error("Analysis requires a single input path.")
        raise RuntimeError("Analysis requires a single input path.")
        return []

    argstr = replace_keywords(CONTEXT, argstr)

    analysisargs = argstr.split(" ")
    output_directory = analysisargs[analysisargs.index("-o") + 1]
    cmd = ["mkdir", "-p", output_directory]
    logger.info(" ".join(cmd))
    subprocess.run(cmd)

    cmd = f"/home/cosmic/anaconda3/envs/cosmic_vla/bin/python3 upchan_coherence.py -d {inputs[0]} {' '.join(analysisargs)}"

    env_base = os.environ.copy()
    env_base.update(common.env_str_to_dict(env))

    logger.info(cmd)
    analysis_output = subprocess.run(
        cmd,
        env=env_base,
        capture_output=True,
        shell=True,
        cwd="/home/cosmic/dev/COSMIC-VLA-CalibrationEngine/"
    )
    if analysis_output.returncode != 0:
        raise RuntimeError(analysis_output.stderr.decode())
    analysis_output = analysis_output.stdout.decode().strip()
    logger.info(analysis_output)

    if 'SLACK_BOT_TOKEN' in env_base:
        from slack_sdk import WebClient
        client = WebClient(token=env_base['SLACK_BOT_TOKEN'])
        channel = env_base.get("SLACK_BOT_CHANNELID", "C03P8DPQHU2")
        thread_ts = env_base.get("SLACK_BOT_THREADTS", None)

        is_tuning_0 = len(re.findall(r'RAWFILE processed was for tuning 0', analysis_output)) > 0
        if is_tuning_0:
            refant_msg = re.findall(r'Using reference antenna.*', analysis_output)
            client.chat_postMessage(
                channel=channel,
                text=f"""
                Processing recorded raw file
                {refant_msg[0]}...
                """,
                thread_ts=thread_ts,
            )

            outputdir = re.findall(r'SAVING products to.*',analysis_output)
            outputdir = [i.replace('SAVING products to ', '') for i in outputdir]
            fixed_delays = re.findall(r'Saving new fixed delays to.*', analysis_output)
            fixed_delays = [i.replace('Saving new fixed delays to: ', '') for i in fixed_delays]
            client.chat_postMessage(
                channel=channel,
                text=f"""
                Raw calibration for is complete.
                upchan_coherence products saved in 
                `{outputdir[0]}`
                New fixed delays are at:
                `{fixed_delays[0]}`""",
                thread_ts=thread_ts,
            )

    return []

if __name__ == "__main__":
    print(run(
        "-f 128 -i 1 -td -b 0.0625 -bc 0.5 -l /mnt/slow/operations/share/telinfo_vla.toml",
        ["/mnt/buf1/delay_calib_test/GUPPI/guppi_59898_12659_38197872229_3C48_0001.0000.raw"],
        "CONDA_PYTHON_EXE:/home/svarghes/anaconda3/envs/turboseti/bin/python3",
    )[0])
