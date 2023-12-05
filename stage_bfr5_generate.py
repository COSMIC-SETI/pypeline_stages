#!/usr/bin/env python
import logging, os, argparse, json, glob
import time
import tomli_w

import bfr5genie
from bfr5genie import entrypoints
from guppi import GuppiRawHandler
import redis

import common

ENV_KEY = "BFR5GenerateENV"
ARG_KEY = "BFR5GenerateARG"
INP_KEY = "BFR5GenerateINP"
NAME = "bfr5_generate"


def dump_telescope_info(
    output_path,
    antenna_properties,
    array_configuration = None,
    timestamp = time.time(),
    dataset_id = None,
    logger=None
):
    telinfo_dict = {
        "telescope_name": "VLA",
        # Geodetic location of telescope reference point.  `latitude` and `longitude`
        # may be given in decimal degrees as a float, or as a sexagesimal string with
        # `:` separators.
        # `altitude` is in meters above the geodetic reference ellipsoid (probably
        # WGS84, but the UVH5 spec is unclear on this point).
        "latitude": "34:04:43.0",
        "longitude": "-107:37:04.0",
        "altitude": 2124,

        # Default diameter for antennas
        # Not needed if all `antennas` entries have `diameter` field.
        "antenna_diameter": 25,

        # Reference frame for the antenna positions.  Can be `ecef` for the ITRF (i.e.
        # Earth-Centered-Earth-Fixed XYZ) frame or `enu` for a topocentric
        # East-North-Up frame with origin at the telescope reference point. If not
        # given, it will be assumed to be `ecef` unless the magnitude of the first
        # antenna"s position vector is less than 6 million meters in which case it will
        # be assumed to be `enu`.  Best practice is to explicitly specify this.
        # This is not case-sensitive.
        "antenna_position_frame": "xyz",
        # List of antennas.  Each entry is a hash containing keys:
        #
        #   - `name`: A string value of the telescope name
        #   - `number`: An integer number identifying the telescope
        #   - `position`: A 3 elements array of floats giving the position in meters.
        #   - `diameter`: A float value for the diameter of the antenna
        #
        # The reference frame of the positions is given in `antennas_position_frame`.
        # The `diameter` entry is optional if a global `antenna_diameter` is given and
        # has the correct value for the antenna.
        "antennas": [],

        "extra_info": {
            "array_configuration": array_configuration,
            "timestamp": timestamp,
            "dataset_id": dataset_id,
        }
    }

    if not isinstance(antenna_properties, list):
        raise ValueError(f"Expected a list, got {type(antenna_properties)}: {antenna_properties}")
    for antProps in antenna_properties:
        if not isinstance(antProps, dict):
            raise ValueError(f"Expected a dict, got {type(antProps)}: {antProps}")
        if "X" not in antProps:
            if logger is not None:
                logger.debug(f"Antenna Properties incomplete indicating out-of-service antenna: {antProps}")
            continue

        telinfo_dict["antennas"].append({
            "name": antProps["name"],
            "number": antProps["widarID"]["value"],
            "diameter": antProps["diameter"]["value"],
            "position": [antProps["X"]["value"], antProps["Y"]["value"], antProps["Z"]["value"]]
        })
    with open(output_path, "wb") as fio:
        tomli_w.dump(telinfo_dict, fio)


def retroactive_setup_for_target_gen(arg_values, inputs, logger=None):
    # creates telinfo file, requests targets and manipulates arg_values
    parser = entrypoints._base_arguments_parser()
    entrypoints._add_arguments_targetselector(parser)
    arg_namespace = parser.parse_args(arg_values+inputs)

    rawfile_dir, rawfile_name = os.path.split(inputs[0])

    grh = GuppiRawHandler(inputs[0])
    grh.open_next_file()
    hdr = grh.read_next_header()

    obsid = hdr.get("OBSID", None)
    if obsid is None:
        obsid = re.match(r"(.*)\.(AC|BD)\.C\d{3,4}\.\d{4}\.raw", rawfile_name).group(1)

    obsmeta_filepath = f"/home/cosmic/dev/logs/obs_meta/{obsid}_metadata.json"
    telinfo_output_path = os.path.join(rawfile_dir, f"{obsid}.telinfo.toml")

    with open(obsmeta_filepath, "r") as fio:
        metadata = json.load(fio)
        if logger is not None:
            logger.debug(f"json.load('{obsmeta_filepath}'): {metadata}")

        dump_telescope_info(
            telinfo_output_path,
            metadata["META_ANT"]["AntennaProperties"],
            metadata["META_ANT"]["datasetId"],
            metadata["META_ANT"]["creation"],
            metadata["META_ANT"]["configuration"],
        )
    
    # issue a single set of targets based on OBSSTART
    pktindex = hdr.get("SYNCTIME", 0) + hdr[arg_namespace.targets_redis_key_timestamp_rawkey]
    output_id = "{}:{}:{}".format(
        "VLA-COSMIC",  # obsid part 1 (telescope_id)
        "offline_array",  # obsid part 2 (subarray_name)
        pktindex,  # obsid part 3
    )
    target_selector_request = "{}:{}:{}:{}:{}".format(
        output_id,
        hdr.source_name,
        hdr.rightascension_hours*180/12,# to degrees
        hdr.declination_degrees,
        hdr.observed_frequency,
    )
    if logger is not None:
        logger.debug(f"Requesting targets from the target-selector: '{target_selector_request}'")


    redis_obj = redis.Redis(host=arg_namespace.redis_hostname, port=arg_namespace.redis_port)
    redis_obj.publish(
        "target-selector:new-pointing",
        target_selector_request
    )
    time.sleep(1)
    argstr_value_additions = [
        "-t", telinfo_output_path,
        "--targets-redis-key-prefix", "targets:VLA-COSMIC:offline_array",
        "--targets-redis-key-timestamp", str(pktindex)
    ]
    if logger is not None:
        logger.debug(f"Appending the following override arguments: {argstr_value_additions}")
    arg_values += argstr_value_additions


def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) != 1:
        raw_filepaths = list(
            filter(
                lambda s: s.endswith(".raw"),
                inputs
            )
        )
        if len(raw_filepaths) != len(inputs):
            logger.warning(f"{NAME} only takes RAW filepaths, filtering.")
        inputs = raw_filepaths


    bfr5genie.logger.handlers.clear()
    for handler in logger.handlers:
        bfr5genie.logger.addHandler(handler)

    arg_values = common.split_argument_string(argstr)
    logger.debug(f"arg_values: {arg_values}")
    arg_values += inputs

    if any(arg in arg_values for arg in ['--take-targets', '--target']):
        env_dict = common.env_str_to_dict(env)
        if env_dict.get("RETROACTIVE_MODE", None) is not None:
            logger.info("Retroactive setup...")
            retroactive_setup_for_target_gen(arg_values, inputs, logger=logger)

        return [entrypoints.generate_targets_for_raw(arg_values)]
    elif any(arg in arg_values for arg in ['--raster-ra']):
        return [entrypoints.generate_raster_for_raw(arg_values)]
    else:
        return [entrypoints.generate_for_raw(arg_values)]

if __name__ == "__main__":
    import sys
    logger = logging.getLogger(NAME)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.DEBUG)

    # args = [
    #     "--telescope-info-toml-filepath",
    #     "/home/cosmic/src/telinfo_vla.toml",
    #     "--targets-redis-key-prefix", "targets:VLA-COSMIC:vlass_array",
    #     "--take-targets", "0",
    #     "--target", '"Voyager 1"',
    #     "/mnt/buf0/test/GUPPI/20A-346.sb43317053.eb43427915.59964.7706725926.70.1_AC_8BIT.0000.raw"
    # ]
    # args = "--telescope-info-toml-filepath /home/cosmic/conf/telinfo_vla.toml --targets-redis-key-prefix targets:VLA-COSMIC:vla_array --beam 11:30:14.5176,07:35:18.257,K2-18b --take-targets 3 --targets-redis-key-timestamp-rawkey PKTSTART".split(" ")
    # args.append("/mnt/buf0/K2-18b_test/GUPPI/TEST_23B-307_S_001.60215.85574990741.3.1.AC.C384.0000.raw")
    
    # if len(sys.argv) > 1:
    #     args = sys.argv[1:]
    # else:
    #     logger.warning(f"Using default arguments: {args}")

    # print(
    #     run(
    #         " ".join(args[0:-1]),
    #         args[-1:],
    #         None
    #     )
    # )
    print(
        run(
            " ".join([
                "--telescope-info-toml-filepath", "/home/cosmic/conf/telinfo_vla.toml",
                "--take-targets", "5"
            ]),
            [
                "/mnt/cosmic-storage-1/data1/unprocessed_vlass/VLASS3.1.sb43872974.eb44055878.60100.3633975.111.1.AC.C672.0000.raw",
                "/mnt/cosmic-storage-1/data1/unprocessed_vlass/VLASS3.1.sb43872974.eb44055878.60100.3633975.111.1.AC.C672.0001.raw",
                "/mnt/cosmic-storage-1/data1/unprocessed_vlass/VLASS3.1.sb43872974.eb44055878.60100.3633975.111.1.AC.C672.0002.raw",
                "/mnt/cosmic-storage-1/data1/unprocessed_vlass/VLASS3.1.sb43872974.eb44055878.60100.3633975.111.1.AC.C672.0003.raw",
                "/mnt/cosmic-storage-1/data1/unprocessed_vlass/VLASS3.1.sb43872974.eb44055878.60100.3633975.111.1.AC.C672.0004.raw",
                "/mnt/cosmic-storage-1/data1/unprocessed_vlass/VLASS3.1.sb43872974.eb44055878.60100.3633975.111.1.AC.C672.0005.raw",
                "/mnt/cosmic-storage-1/data1/unprocessed_vlass/VLASS3.1.sb43872974.eb44055878.60100.3633975.111.1.AC.C672.0006.raw",
            ],
            "RETROACTIVE_MODE=true"
        )
    )
