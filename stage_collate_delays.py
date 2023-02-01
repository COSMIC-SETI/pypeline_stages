import pandas
import time
import os
import argparse
import logging

ENV_KEY = None
ARG_KEY = "CollateDelaysARG"
INP_KEY = "CollateDelaysINP"
NAME = "Collate Delays"


def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) != 1:
        raise RuntimeError("Provide a delay file!")
    
    parser = argparse.ArgumentParser(
        description="A module to collate delays as measured by upchan_coherence (vla-analysis stage).",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-b",
        "--baseband-pair-id",
        type=int,
        required=True,
        help="0 = AC, 1 = BD.",
    )
    parser.add_argument(
        "-r",
        "--reference-antname",
        type=str,
        required=True,
        help="The name of the antenna used as reference (only baselines including this will be collated).",
    )
    parser.add_argument(
        "-c",
        "--collation-directory",
        type=str,
        required=True,
        help="The output directory for the collation (delays_collated_{baseband}_v4.csv).",
    )
    args = parser.parse_args(argstr.split(" "))

    with open(inputs[0], 'rb') as fio:
        dataframe = pandas.read_csv(
            fio,
            usecols=['Baseline', 'total_pol0', 'total_pol1', 'geo', 'non-geo_pol0', 'non-geo_pol1', 'snr_pol0', 'snr_pol1']
        )
        reference_baseline_rows = list(map(
            lambda b: args.reference_antname in b,
            dataframe['Baseline']
        ))


    baseband = 'AC' if args.baseband_pair_id == 0 else 'BD'
    output_filepath = os.path.join(args.collation_directory, f"delays_collated_{baseband}_v4.csv")
    os.makedirs(args.collation_directory, exist_ok = True)

    if not os.path.exists(output_filepath):
        with open(output_filepath, 'w') as fio:
            fio.write('baseline,reference,timestamp,total_pol0,total_pol1,geo,non-geo_pol0,non-geo_pol1,snr_pol0,snr_pol1,origin-filepath\n')

    timestamp = f"{time.time()}"
    with open(output_filepath, 'a') as fio:
        for _, row in dataframe[reference_baseline_rows].iterrows():
            fio.write(
                ','.join([
                    row['Baseline'],
                    args.reference_antname,
                    timestamp,
                    f"{row['total_pol0']:> 12.03f}",
                    f"{row['total_pol1']:> 12.03f}",
                    f"{row['geo']:> 12.03f}",
                    f"{row['non-geo_pol0']:> 12.03f}",
                    f"{row['non-geo_pol1']:> 12.03f}",
                    f"{row['snr_pol0']:> 12.03f}",
                    f"{row['snr_pol1']:> 12.03f}",
                    inputs[0]
                ])+'\n'
            )
    
    return output_filepath

if __name__ == "__main__":
    print(
        run(
            "-b 1 -r ea10 -c .",
            ["/mnt/slow/vla_analysis_plots/guppi_59898_63357_38199032898_1331+305_0001/1/delays_guppi_59898_63357_38199032898_1331+305_0001.0000.raw_8584.000-8592.000.csv"],
            None
        )
    )
