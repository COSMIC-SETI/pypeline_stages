#!/usr/bin/env python
import logging, os, argparse, json, glob
from datetime import datetime

import tomli as tomllib # `tomllib` as of Python 3.11 (PEP 680)
import h5py
import numpy
import redis

import bfr5_aux

ENV_KEY = None
ARG_KEY = "BFR5GenerateARG"
INP_KEY = "BFR5GenerateINP"
NAME = "bfr5_generate"


def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) != 1:
        logger.error("bfr5_generate requires one input, the RAW filepath.")
        return None
    
    parser = argparse.ArgumentParser(
        description="A script that generates a BFR5 file.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument(
        "-t",
        "--telescope-info-toml-filepath",
        type=str,
        required=True,
        help="The path to telescope information.",
    )
    parser.add_argument(
        "--output-filepath",
        type=str,
        default=None,
        help="The path to which the output will be written (instead of alongside the raw_filepath).",
    )
    parser.add_argument(
        "-b",
        "--beam",
        default=None,
        action="append",
        metavar=("ra_hour,dec_deg[,name]"),
        help="The coordinates of a beam (optionally the name too)."
    )
    parser.add_argument(
        "-m",
        "--mitigate-antenna",
        default=[],
        nargs="+",
        help="The names of antenna to migigate (associated calibration coefficients are set to zero)."
    )
    parser.add_argument(
        "-p",
        "--phase-center",
        default=None,
        metavar=("ra_hour,dec_deg"),
        help="The coordinates of the data's phase-center."
    )
    parser.add_argument(
        "--redis-hostname",
        type=str,
        default="redishost",
        help="The hostname of the Redis server.",
    )
    parser.add_argument(
        "--redis-port",
        type=int,
        default=6379,
        help="The port of the Redis server.",
    )
    parser.add_argument(
        "--targets-redis-key-prefix",
        type=str,
        default="targets:VLA-COSMIC:vlass_array",
        help="The prefix of the redis key holding targets in JSON."
    )
    parser.add_argument(
        "--targets-redis-key-timestamp",
        type=str,
        default=None,
        help="The identifying timestamp of the redis key holding targets in JSON. If not supplied, it is ascertained from the starting time of the raw file."
    )
    parser.add_argument(
        "--take-targets",
        type=int,
        default=0,
        help="The number of targets to form beams on from the redis key."
    )
    parser.add_argument(
        "--take-targets-after",
        type=int,
        default=1,
        help="The number of targets to skip. Typically 1 to skip the first target which is phase-center."
    )
    parser.add_argument(
        "raw_filepaths",
        type=str,
        nargs="+",
        help="The path to the GUPPI RAW file stem or of all files.",
    )
    args = parser.parse_args(argstr.split(" ") + inputs)

    if len(args.raw_filepaths) == 1 and not os.path.exists(args.raw_filepaths[0]):
        logger.info(f"Given RAW filepath does not exist, assuming it is the stem.")
        args.raw_filepaths = glob.glob(f"{args.raw_filepaths[0]}*.raw")
        logger.info(f"Found {args.raw_filepaths}.")

    raw_blocks = 0
    raw_header = {}
    for raw_file_enum, raw_filepath in enumerate(args.raw_filepaths):
        with open(raw_filepath, mode="rb") as f:
            header_entry = f.read(80).decode()
            while header_entry:
                if header_entry == "END" + " "*77:
                    break
                
                if raw_file_enum != 0:
                    header_entry = f.read(80).decode()
                    continue

                key = header_entry[0:8].strip()
                value = header_entry[9:].strip()
                try:
                    value = float(value)
                    if value == int(value):
                        value = int(value)
                except:
                    # must be a str value, drop enclosing single-quotes
                    assert value[0] == value[-1] == "'"
                    value = value[1:-1].strip()

                raw_header[key] = value
                header_entry = f.read(80).decode()

            logger.debug(f"First header: {raw_header} (ends @ position {f.tell()})")

            # count number of blocks in file, assume BLOCSIZE is consistent
            data_seek_size = raw_header["BLOCSIZE"]
            if raw_header.get("DIRECTIO", 0) == 1:
                data_seek_size = int((data_seek_size + 511) / 512) * 512
                logger.debug(f"BLOCSIZE rounded {raw_header['BLOCSIZE']} up to {data_seek_size}")

            while True:
                if raw_header.get("DIRECTIO", 0) == 1:
                    origin = f.tell()
                    f.seek(int((f.tell() + 511) / 512) * 512)
                    logger.debug(f"Seeked past padding: {origin} -> {f.tell()}")

                f.seek(data_seek_size + f.tell())
                block_header_start = f.tell()
                raw_blocks += 1
                try:
                    header_entry = f.read(80).decode()
                    if len(header_entry) < 80:
                        break
                    while header_entry != "END" + " "*77:
                        header_entry = f.read(80).decode()
                except UnicodeDecodeError as err:
                    pos = f.tell()
                    f.seek(pos - 321)
                    preceeding_bytes = f.read(240)
                    next_bytes = f.read(240)
                    
                    logger.error(f"UnicodeDecodeError in {raw_filepath} at position: {pos}")
                    logger.error(f"Preceeding bytes: {preceeding_bytes}")
                    logger.error(f"Proceeding bytes: {next_bytes}")
                    logger.error(f"Block #{raw_blocks} starting at {block_header_start}")

                    exit(1)

    logger.info(f"Counted {raw_blocks} block(s).")

    # General RAW metadata
    nants = raw_header.get("NANTS", 1)
    npol = raw_header["NPOL"]
    nchan = raw_header["OBSNCHAN"] // nants
    schan = raw_header.get("SCHAN", 0)
    ntimes = (raw_header["BLOCSIZE"] * 8) // (raw_header["OBSNCHAN"] * raw_header["NPOL"] * 2 * raw_header["NBITS"])
    antenna_names = []
    for i in range(100):
        key = f"ANTNMS{i:02d}"
        if key in raw_header:
            # antenna_names += map(lambda x: x[:-1], raw_header[key].split(",")) 
            antenna_names += raw_header[key].split(",")

    antenna_names[0:nants]
    logger.debug(f"RAW antenna_names: {antenna_names}")
    
    # Telescope information

    with open(args.telescope_info_toml_filepath, mode="rb") as f:
        telescope_info = tomllib.load(f)

    logger.debug(f"TOML antenna_names: {[antenna['name'] for antenna in telescope_info['antennas']]}")
    telescope_antenna_names = [antenna["name"] for antenna in telescope_info["antennas"] if antenna["name"] in antenna_names]
    logger.debug(f"telescope_antenna_names: {telescope_antenna_names}")
    assert all(name in telescope_antenna_names for name in antenna_names)

    antenna_positions = numpy.array([antenna["position"] for antenna in telescope_info["antennas"] if antenna["name"] in antenna_names])
    antenna_position_frame = telescope_info["antenna_position_frame"]
    telescope_longitude = bfr5_aux.degrees_process(telescope_info["longitude"])
    telescope_latitude = bfr5_aux.degrees_process(telescope_info["latitude"])
    telescope_altitude = telescope_info["altitude"]

    if 'xyz' == antenna_position_frame.lower():
        logger.info("Transforming antenna positions from XYZ to ECEF")
        bfr5_aux.transform_antenna_positions_xyz_to_ecef(
            telescope_longitude,
            telescope_latitude,
            telescope_altitude,
            antenna_positions,
        )
    else:
        # TODO handle enu
        assert antenna_position_frame.lower() == 'ecef'

    antenna_position_frame = 'ecef'

    input_dir, input_filename = os.path.split(args.raw_filepaths[0])
    if args.output_filepath is None:
        output_filepath = os.path.join(input_dir, f"{os.path.splitext(input_filename)[0]}.bfr5")
    else:
        output_filepath = args.output_filepath
    logger.info(f"Output filepath: {output_filepath}")
    
    # Phasor calculation stuff

    start_time_unix = raw_header["SYNCTIME"] + raw_header["PKTIDX"] * raw_header.get("TBIN", 1/raw_header["CHAN_BW"]) * ntimes/raw_header.get("PIPERBLK", ntimes)
    block_time_span_s = raw_header.get("PIPERBLK", ntimes) * raw_header.get("TBIN", 1/raw_header["CHAN_BW"]) * ntimes/raw_header.get("PIPERBLK", ntimes)

    if args.phase_center is None:
        phase_center_ra = raw_header.get("RA_PHAS", raw_header["RA_STR"])
        phase_center_dec = raw_header.get("DEC_PHAS", raw_header["DEC_STR"])
    else:
        (phase_center_ra, phase_center_dec) = args.phase_center.split(',')

    phase_center = bfr5_aux.SkyCoord(
        float(phase_center_ra) * numpy.pi / 12.0 ,
        float(phase_center_dec) * numpy.pi / 180.0 ,
        unit='rad'
    )

    primary_center = bfr5_aux.SkyCoord(
        float(raw_header.get("RA_PRIM", raw_header["RA_STR"])) * numpy.pi / 12.0 ,
        float(raw_header.get("DEC_PRIM", raw_header["DEC_STR"])) * numpy.pi / 180.0 ,
        unit='rad'
    )

    # find the observation channel0 frequency, then offset to the recorded subband channel0
    frequency_channel_0_hz = raw_header["OBSFREQ"] - ((raw_header.get("FENCHAN", nchan)/2.0 + 0.5)  * raw_header["CHAN_BW"])
    frequency_channel_0_hz += schan * raw_header["CHAN_BW"]
    frequencies_hz = frequency_channel_0_hz + numpy.arange(nchan)*raw_header["CHAN_BW"]
    assert len(frequencies_hz) == nchan

    times_unix = (start_time_unix + 0.5 * block_time_span_s) + numpy.arange(raw_blocks)*block_time_span_s

    beam_strs = []
    if args.take_targets != 0:
        redis_obj = redis.Redis(host=args.redis_hostname, port=args.redis_port)
        if args.targets_redis_key_timestamp is None:
            file_start_packet_index = raw_header["SYNCTIME"] + raw_header["PKTIDX"]
            logger.info(f"Targets key timestamp is taken to be the starting packet-index of the file: {file_start_packet_index}")
            args.targets_redis_key_timestamp = file_start_packet_index

        targets_redis_key = f"{args.targets_redis_key_prefix}:{args.targets_redis_key_timestamp}"
        logger.info(f"Accessing targets at {targets_redis_key}.")
        targets = redis_obj.get(targets_redis_key)
        if targets is None:
            raise ValueError(f"No targets to retrieve at: {targets_redis_key}.")

        targets = json.loads(targets)

        for target in targets[args.take_targets_after : args.take_targets_after+args.take_targets]:
            beam_strs.append(
                f"{target['ra']*24/360},{target['dec']},{target['source_id']}"
            )

        if len(beam_strs) < args.take_targets:
            logger.warning(f"Could only take {len(beam_strs)} targets.")
    elif args.beam is None:
        # scrape from RAW file RA_OFF%01d,DEC_OFF%01d
        key_enum = 0
        while True:
            ra_key = f"RA_OFF{key_enum}"
            dec_key = f"DEC_OFF{key_enum}"
            if not (ra_key in raw_header and dec_key in raw_header):
                break
            
            beam_strs.append(f"{raw_header[ra_key]},{raw_header[dec_key]},BEAM_{key_enum}")

            key_enum += 1
            if key_enum == 10:
                break

        logger.info(f"Collected {key_enum} beam coordinates from the RAW header, in lieu of CLI provided beam coordinates.")
    elif len(args.beam) > 0:
        logger.info(args.beam)
        beam_strs = list(b for b in args.beam)

    beams = {}
    for i, beam_str in enumerate(beam_strs):
        coords = beam_str.split(',')
        if len(coords) == 3:
            beam_name = coords[-1]
        else:
            beam_name = f"BEAM_{i}"
        beams[beam_name] = bfr5_aux.SkyCoord(
            float(coords[0]) * numpy.pi / 12.0,
            float(coords[1]) * numpy.pi / 180.0,
            unit='rad'
        )

    nbeams = len(beams)
    if nbeams == 0:
        logger.warning(f"No beam coordinates provided, forming a beam on phase-center.")
        beams["PHASE_CENTER"] = phase_center
        nbeams = 1
        
    beam_coordinates_string = "\n\t".join(f"{k}: {v}".replace("\n", "") for k, v in beams.items())
    logger.info(f"Beam coordinates:\n\t{beam_coordinates_string}")

    calibrationCoefficients = numpy.ones(
        (schan + nchan, npol, nants)
    )*(1+0j)
    for ant_name in args.mitigate_antenna:
        if ant_name not in telescope_antenna_names:
            raise RuntimeError(f"Cannot mitigate antenna {ant_name} as it is not recorded in the file. Antenna names: {telescope_antenna_names}")
        index = telescope_antenna_names.index(ant_name)
        calibrationCoefficients[:, :, index] *= 0

    _, delay_ns = bfr5_aux.phasors(
        antenna_positions,
        phase_center,
        numpy.array(list(beams.values())),
        times_unix,
        frequencies_hz,
        calibrationCoefficients[schan:,:,:],
        (
            telescope_longitude,
            telescope_latitude,
            telescope_altitude
        ),
        referenceAntennaIndex = 0
    )

    with h5py.File(output_filepath, "w") as f:
        dimInfo = f.create_group("diminfo")
        dimInfo.create_dataset("nants", data=nants)
        dimInfo.create_dataset("npol", data=npol)
        dimInfo.create_dataset("nchan", data=schan+nchan)
        dimInfo.create_dataset("nbeams", data=nbeams)
        dimInfo.create_dataset("ntimes", data=ntimes)

        beamInfo = f.create_group("beaminfo")
        beamInfo.create_dataset("ras", data=numpy.array([beam.ra.rad for beam in beams.values()]), dtype='f') # radians
        beamInfo.create_dataset("decs", data=numpy.array([beam.dec.rad for beam in beams.values()]), dtype='f') # radians
        source_names = [beam.encode() for beam in beams.keys()]
        longest_source_name = max(len(name) for name in source_names)
        beamInfo.create_dataset("src_names", data=numpy.array(source_names, dtype=f"S{longest_source_name}"), dtype=h5py.special_dtype(vlen=str))

        calInfo = f.create_group("calinfo")
        calInfo.create_dataset("refant", data=raw_header.get("REFANT", raw_header["ANTNMS00"].split(',')[0]).encode())
        calInfo.create_dataset("cal_K", data=numpy.ones((npol, nants)), dtype='f')
        calInfo.create_dataset("cal_B", data=numpy.ones((schan+nchan, npol, nants))*(1+0j), dtype='F')
        calInfo.create_dataset("cal_G", data=numpy.ones((npol, nants))*(1+0j), dtype='F')
        calInfo.create_dataset("cal_all", data=calibrationCoefficients, dtype='F')

        delayInfo = f.create_group("delayinfo")
        delayInfo.create_dataset("delays", data=delay_ns, dtype='f')
        delayInfo.create_dataset("rates", data=numpy.zeros((ntimes, nbeams, nants)), dtype='f')
        delayInfo.create_dataset("time_array", data=times_unix, dtype='f')
        delayInfo.create_dataset("jds", data=(times_unix/86400) + 2440587.5, dtype='f')
        delayInfo.create_dataset("dut1", data=raw_header.get("DUT1", 0.0), dtype='f')

        obsInfo = f.create_group("obsinfo")
        obsInfo.create_dataset("obsid", data=raw_header.get("OBSID", "Unknown").encode())
        obsInfo.create_dataset("freq_array", data=frequencies_hz*1e-9, dtype='f') # GHz
        obsInfo.create_dataset("phase_center_ra", data=phase_center.ra.rad, dtype='f') # radians
        obsInfo.create_dataset("phase_center_dec", data=phase_center.dec.rad, dtype='f') # radians
        obsInfo.create_dataset("primary_center_ra", data=primary_center.ra.rad, dtype='f') # radians
        obsInfo.create_dataset("primary_center_dec", data=primary_center.dec.rad, dtype='f') # radians
        obsInfo.create_dataset("instrument_name", data="COSMIC".encode())

        telInfo = f.create_group("telinfo")
        telInfo.create_dataset("antenna_positions", data=antenna_positions, dtype='f')
        telInfo.create_dataset("antenna_position_frame", data=antenna_position_frame.encode())
        longest_antenna_name = max(*[len(name) for name in telescope_antenna_names])
        telInfo.create_dataset("antenna_names", data=numpy.array(telescope_antenna_names, dtype=f"S{longest_antenna_name}"), dtype=h5py.special_dtype(vlen=str))
        telInfo.create_dataset("antenna_numbers", data=numpy.array([antenna["number"] for antenna in telescope_info["antennas"] if antenna["name"] in antenna_names]), dtype='i')
        telInfo.create_dataset("antenna_diameters", data=numpy.array([antenna.get("diameter", telescope_info.get("antenna_diameter", 0.0)) for antenna in telescope_info["antennas"] if antenna["name"] in antenna_names]), dtype='f')
        telInfo.create_dataset("latitude", data=telescope_latitude, dtype='f')
        telInfo.create_dataset("longitude", data=telescope_longitude, dtype='f')
        telInfo.create_dataset("altitude", data=telescope_altitude, dtype='f')
        telInfo.create_dataset("telescope_name", data=telescope_info["telescope_name"].encode())
    
    return [output_filepath]

if __name__ == "__main__":
    import sys
    logger = logging.getLogger(NAME)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)

    args = [
        "--telescope-info-toml-filepath",
        "/home/cosmic/src/telinfo_vla.toml",
        "/mnt/buf0/mydonsol_blade/bladetest_vlass_32c_128k.0000.raw"
    ]
    
    if len(sys.argv) > 1:
        args = sys.argv[1:]
    else:
        logger.warning(f"Using default arguments: {args}")

    print(
        run(
            " ".join(args[0:-1]),
            args[-1:],
            None
        )
    )
