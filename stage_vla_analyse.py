import subprocess
import os
import re
import logging

PROC_ENV_KEY = "CorrelationAnalysisENV"
PROC_ARG_KEY = "CorrelationAnalysisARG"
PROC_INP_KEY = "CorrelationAnalysisINP"
PROC_NAME = "vla_analyse"

ENV_KEY = "CorrelationAnalysisENV"
ARG_KEY = "CorrelationAnalysisARG"
INP_KEY = "CorrelationAnalysisINP"
NAME = "vla_analyse"

CONTEXT = {
    "OBSSTEM": ""
}

def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) == 0:
        logging.error("Analysis requires a single input path.")
        raise RuntimeError("Analysis requires a single input path.")
        return []

    if " -o " not in argstr:
        argstr += " -o /mnt/slow/vla_analysis_plots"

    argstr = argstr.replace("$OBSSTEM$", CONTEXT["OBSSTEM"])

    analysisargs = argstr.split(" ")
    output_directory = analysisargs[analysisargs.index("-o") + 1]
    cmd = ["mkdir", "-p", output_directory]
    logger.info(" ".join(cmd))
    subprocess.run(cmd)

    cmd = f"python3 upchan_coherence.py -d {inputs[0]} {' '.join(analysisargs)}"

    env_base = os.environ.copy()
    if env is not None:
        for variablevalues in env.split(" "):
            if ":" in variablevalues:
                pair = variablevalues.split(":")
                env_base[pair[0]] = pair[1]

    logger.info(cmd)
    analysis_output = subprocess.run(
        cmd,
        env=env_base,
        capture_output=True,
        shell=True,
        cwd="/home/cosmic/dev/FrontPage/Nodes/GPU-Compute/raw_correlation_analysis/"
    )
    if analysis_output.returncode != 0:
        raise RuntimeError(analysis_output.stderr.decode())
    analysis_output = analysis_output.stdout.decode().strip()
    logger.info(analysis_output)

    if 'SLACK_BOT_TOKEN' in env_base:
        baseline_timedelays = {}
        baseline_min_timedelays = {}
        for m in re.findall(r'Time delay for (?P<baseline>.*?): (?P<timedelay>.*)', analysis_output):
            timedelay_match = re.match(r'\((.*?), (.*?)\).*', m[1])
            baseline_timedelays[m[0]] = m[1]
            baseline_min_timedelays[m[0]] = min(abs(float(timedelay_match.group(1))), abs(float(timedelay_match.group(2))))

        plot_id = re.search(r'Plotted: (.*)', analysis_output)[1]
        baselines_ascending_timedelays = []
        for baseline, timedelay in baseline_min_timedelays.items():
            index = 0
            for baseline_asc in baselines_ascending_timedelays:
                if baseline_min_timedelays[baseline_asc] > timedelay:
                    break
                index += 1
            baselines_ascending_timedelays.insert(index, baseline)
        
        from slack_sdk import WebClient
        client = WebClient(token=env_base['SLACK_BOT_TOKEN'])
        channel = env_base.get("SLACK_BOT_CHANNELID", "C03P8DPQHU2")
        thread_ts = env_base.get("SLACK_BOT_THREADTS", None)

        result = client.files_upload(
            channels = channel,
            file = os.path.join(output_directory, f"auto_corr_{plot_id}.png"),
            filename =  f"auto_corr_{plot_id}.png",
            title = plot_id,
            initial_comment = f"{plot_id} @ {output_directory}",
            thread_ts = thread_ts
        )
        assert result["ok"]
        if thread_ts is None:
            thread_ts = result["file"]["shares"]["public"][channel][0]["ts"]

        delays_output = os.path.join(output_directory, f"delays_{plot_id}.csv")
        result = client.files_upload(
            channels = channel,
            file = delays_output,
            filename =  f"delays_{plot_id}.csv",
            title = f"delays {plot_id}",
            initial_comment = f"Delays {plot_id} @ {output_directory}",
            thread_ts = thread_ts
        )
        assert result["ok"]

        for baseline in baselines_ascending_timedelays[:min(10, len(baseline_min_timedelays))]:
            if not client.files_upload(
                channels = channel,
                file = os.path.join(output_directory, f"cross_corr_{plot_id}_{baseline}.png"),
                filename =  f"cross_corr_{plot_id}_{baseline}.png",
                title = f"{baseline} {plot_id}",
                initial_comment = f"{baseline}",
                thread_ts = thread_ts,
            )["ok"]:
                break

    return [delays_output]

if __name__ == "__main__":
    print(run(
        "-f 128 -i 1 -td -b 0.0625 -bc 0.5 -l /mnt/slow/operations/share/telinfo_vla.toml",
        ["/mnt/buf1/delay_calib_test/GUPPI/guppi_59898_12659_38197872229_3C48_0001.0000.raw"],
        "CONDA_PYTHON_EXE:/home/svarghes/anaconda3/envs/turboseti/bin/python3",
    )[0])
