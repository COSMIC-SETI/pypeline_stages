#!/usr/bin/env python
import logging, os, argparse, json, glob
from datetime import datetime

import h5py

from cosmic_database import entities
from cosmic_database.engine import CosmicDB_Engine

from SeticorePy import viewer as seticore_viewer

import common

ENV_KEY = None
ARG_KEY = "DBArchiveARG"
INP_KEY = "DBArchiveINP"
NAME = "dbarchive"

CONTEXT = {
    "PROJID": None,
    "OBSID": None,
    "DATASET": None,
    "TUNING": None,
    "SCHAN": None,
}

def run(argstr, inputs, env, logger=None):
    if logger is None:
        logger = logging.getLogger(NAME)
    if len(inputs) < 1:
        logger.error("dbarchive requires at least one filepath input: .bfr5[, .hits, .stamps].")
        return None

    parser = argparse.ArgumentParser(
        description="Commit CosmicDB_Beam entities from BFR5 file."
    )
    parser.add_argument(
        "--cosmicdb-engine-conf",
        type=str,
        required=True,
        help="The YAML file path specifying the COSMIC database.",
    )
    parser.add_argument(
        "-d",
        "--destination-dirpath",
        type=str,
        required=True,
        help="The destination directory.",
    )
    if argstr is None:
        argstr = ""
    argstr = replace_keywords(CONTEXT, argstr)
    arglist = [arg for arg in argstr.split(" ") if len(arg) != 0]
    args = parser.parse_args(arglist)

    input_to_output_filepath_map = {
        inputpath: os.path.join(args.destination_dirpath, os.path.basename(inputpath))
        for inputpath in inputs
    }

    bfr5_filepaths = [inp for inp in inputs if inp.endswith(".bfr5")]
    stamps_filepaths = [inp for inp in inputs if inp.endswith(".stamps")]
    hits_filepaths = [inp for inp in inputs if inp.endswith(".hits")]

    if len(bfr5_filepaths) != 1:
        logger.warning(f"Expecting only 1 BFR5 input. Received: {bfr5_filepaths}.")
        
    bfr5 = h5py.File(bfr5_filepaths[0], 'r')

    beam_index_to_db_id_map = {}
    beam_index_to_obs_id_map = {}

    cosmicdb_engine = CosmicDB_Engine(engine_conf_yaml_filepath=args.cosmicdb_engine_conf)
    with cosmicdb_engine.session() as session:
        for beam_i, beam_source in enumerate(bfr5["beaminfo"]["src_names"]):
            scan_id = bfr5["obsinfo"]["obsid"][()].decode()
            beam_time_start = datetime.fromtimestamp(bfr5["delayinf"]["time_array"][0])
            beam_time_end = datetime.fromtimestamp(bfr5["delayinf"]["time_array"][-1])

            db_obs = session.scalars(
                sqlalchemy.select(entities.CosmicDB_Observation)
                .where(
                    entities.CosmicDB_Observation.scan_id == scan_id,
                    entities.CosmicDB_Observation.start <= beam_time_start,
                    entities.CosmicDB_Observation.end >= beam_time_start,
                    entities.CosmicDB_Observation.start <= beam_time_end,
                    entities.CosmicDB_Observation.end >= beam_time_end,
                )
            ).one()

            db_beam_fields = {
                "observation_id": db_obs.id,
                "ra_radians": bfr5["beaminfo"]["ras"][beam_i],
                "dec_radians": bfr5["beaminfo"]["decs"][beam_i],
                "source": beam_source.decode(),
                "start": beam_time_start,
                "end": beam_time_end,
            }

            db_beam = cosmicdb_engine.select_entity(
                session,
                entities.CosmicDB_ObservationBeam,
                **db_beam_fields
            )
            if db_beam is None:
                db_beam = entities.CosmicDB_ObservationBeam(
                    **db_beam_fields
                )
                session.add(db_beam)
                session.commit()
                session.refresh(db_beam)
                logger.info(f"Committed {db_beam}")
            else:
                logger.info(f"Found {db_beam}")
            
            beam_index_to_db_id_map[beam_i] = db_beam.id
            beam_index_to_obs_id_map[beam_i] = db_beam.observation_id
        
        for stamps_filepath in stamps_filepaths:
            for stamp_enum, stamp in enumerate(seticore_viewer.read_stamps(stamps_filepath)):
                session.add(
                    entities.CosmicDB_ObservationStamp(
                        observation_id = beam_index_to_obs_id_map[stamp.beam],
                        tuning = CONTEXT["TUNING"],
                        subband_offset = CONTEXT["SCHAN"],

                        file_uri = input_to_output_filepath_map[stamps_filepath],
                        file_local_enumeration = stamp_enum,

                        source_name = stamp.sourceName,
                        ra_hours = stamp.ra,
                        dec_degrees = stamp.dec,
                        fch1_mhz = stamp.fch1,
                        foff_mhz = stamp.foff,
                        tstart = stamp.tstart,
                        tsamp = stamp.tsamp,
                        telescope_id = stamp.telescopeId,
                        num_timesteps = stamp.numTimesteps,
                        num_channels = stamp.numChannels,
                        num_polarizations = stamp.numPolarizations,
                        num_antennas = stamp.numAntennas,
                        coarse_channel = stamp.coarseChannel,
                        fft_size = stamp.fftSize,
                        start_channel = stamp.startChannel,
                        schan = stamp.schan,
                        obsid = stamp.obsid,

                        signal_frequency = hit.signal.frequency,
                        signal_index = hit.signal.index,
                        signal_drift_steps = hit.signal.driftSteps,
                        signal_drift_rate = hit.signal.driftRate,
                        signal_snr = hit.signal.snr,
                        signal_coarse_channel = hit.signal.coarseChannel,
                        signal_beam = hit.signal.beam,
                        signal_num_timesteps = hit.signal.numTimesteps,
                        signal_power = hit.signal.power,
                        signal_incoherent_power = hit.signal.incoherentPower,

                        beam_id = beam_index_to_db_id_map[hit.signal.beam],
                    )
                )
        session.commit()
        for hits_filepath in hits_filepaths:
            for hit_enum, hit in enumerate(seticore_viewer.read_hits(hits_filepath)):
                session.add(
                    entities.CosmicDB_ObservationHit(
                        beam_id = beam_index_to_db_id_map[hit.signal.beam],
                        observation_id = beam_index_to_obs_id_map[hit.signal.beam],
                        tuning = CONTEXT["TUNING"],
                        subband_offset = CONTEXT["SCHAN"],

                        file_uri = input_to_output_filepath_map[hits_filepath],
                        file_local_enumeration = hit_enum,
                        
                        signal_frequency = hit.signal.frequency,
                        signal_index = hit.signal.index,
                        signal_drift_steps = hit.signal.driftSteps,
                        signal_drift_rate = hit.signal.driftRate,
                        signal_snr = hit.signal.snr,
                        signal_coarse_channel = hit.signal.coarseChannel,
                        signal_beam = hit.signal.beam,
                        signal_num_timesteps = hit.signal.numTimesteps,
                        signal_power = hit.signal.power,
                        signal_incoherent_power = hit.signal.incoherentPower,

                        source_name = hit.filterbank.sourceName,
                        fch1_mhz = hit.filterbank.fch1,
                        foff_mhz = hit.filterbank.foff,
                        tstart = hit.filterbank.tstart,
                        tsamp = hit.filterbank.tsamp,
                        ra_hours = hit.filterbank.ra,
                        dec_degrees = hit.filterbank.dec,
                        telescope_id = hit.filterbank.telescopeId,
                        num_timesteps = hit.filterbank.numTimesteps,
                        num_channels = hit.filterbank.numChannels,
                        coarse_channel = hit.filterbank.coarseChannel,
                        start_channel = hit.filterbank.startChannel,
                    )
                )
        session.commit()

    # Move the files
    if not os.path.exists(args.destination_dirpath):
        logger.info(f"Creating destination directory: {args.destination_dirpath}")
        makedirs(args.destination_dirpath, user="cosmic", group="cosmic", mode=0o777)

    all_moved = []
    for inputpath in inputs:
        destinationpath = input_to_output_filepath_map[inputpath]
        cmd = [
            "mv" if not args.copy else "cp",
            inputpath,
            destinationpath
        ]
        logger.info(" ".join(cmd))
        output = subprocess.run(cmd, capture_output=True)
        if output.returncode != 0:
            raise RuntimeError(output.stderr.decode())
        shutil.chown(destinationpath, user="cosmic", group="cosmic")
            
        all_moved.append(destinationpath)
    return all_moved

if __name__ == "__main__":
    import sys
    logger = logging.getLogger(NAME)
    logger.addHandler(logging.StreamHandler())
    logger.setLevel(logging.INFO)
