#!/usr/bin/env python
import logging, os, argparse, json, glob, subprocess, shutil, math
from datetime import datetime
import h5py
import sqlalchemy

from Pypeline import replace_keywords

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
        "-c",
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

    # supplement CONTEXT values from env-vars in case script is reused
    env_dict = common.env_str_to_dict(env)
    context_keys = list(CONTEXT.keys())
    for key in context_keys:
        CONTEXT[key] = CONTEXT.get(key, env_dict.get(key, None))

    if CONTEXT["SCHAN"] is None:
        logger.warning("No value provided for SCHAN, substituting -1.")
        CONTEXT["SCHAN"] = -1

    beam_index_to_db_id_map = {}
    beam_index_to_obs_id_map = {}

    cosmicdb_engine = CosmicDB_Engine(engine_conf_yaml_filepath=args.cosmicdb_engine_conf)
    with cosmicdb_engine.session() as session:
        beam_src_names = list(map(lambda s: s.decode(), bfr5["beaminfo"]["src_names"][:]))
        beam_ras = list(bfr5["beaminfo"]["ras"][:])
        beam_decs = list(bfr5["beaminfo"]["decs"][:])

        beam_src_names.append("Incoherent")
        beam_ras.append(bfr5["obsinfo"]["phase_center_ra"][()])
        beam_decs.append(bfr5["obsinfo"]["phase_center_dec"][()])

        for beam_i, beam_source in enumerate(beam_src_names):
            scan_id = bfr5["obsinfo"]["obsid"][()].decode()
            beam_time_start = datetime.fromtimestamp(bfr5["delayinfo"]["time_array"][0])
            # floor to the second as teh beam time can extend past the related scan
            beam_time_end = datetime.fromtimestamp(bfr5["delayinfo"]["time_array"][-1]).replace(microsecond=0)

            where_criteria = [
                entities.CosmicDB_Observation.scan_id == scan_id,
                entities.CosmicDB_Observation.start <= beam_time_start,
                entities.CosmicDB_Observation.end >= beam_time_start,
                entities.CosmicDB_Observation.start <= beam_time_end,
                entities.CosmicDB_Observation.end >= beam_time_end,
            ]
            try:
                db_obs = session.scalars(
                    sqlalchemy.select(entities.CosmicDB_Observation)
                    .where(*where_criteria)
                ).one()
            except:
                logger.warning(f"Failed to get Observation with the criteria: scan_id=={scan_id}, start<={beam_time_start}<=end, start<={beam_time_end}<=end.")   
                db_obs = session.scalars(
                    sqlalchemy.select(entities.CosmicDB_Observation)
                    .where(where_criteria[0])
                    .order_by(entities.CosmicDB_Observation.end.desc())
                ).first()
                logger.warning(f"Fallback is the most recent Observation with that scan_id: {db_obs}")

            db_beam_fields = {
                "observation_id": db_obs.id,
                "ra_radians": beam_ras[beam_i],
                "dec_radians": beam_decs[beam_i],
                "source": beam_source,
                "start": beam_time_start,
                "end": beam_time_end,
            }

            db_beam = session.execute(
                sqlalchemy.select(entities.CosmicDB_ObservationBeam)
                .where(*[
                    getattr(entities.CosmicDB_ObservationBeam, colname) == colval
                    for colname, colval in db_beam_fields.items()
                ])
            ).scalars().all()
            if len(db_beam) == 0:
                db_beam = entities.CosmicDB_ObservationBeam(
                    **db_beam_fields
                )
                session.add(db_beam)
                session.commit()
                session.refresh(db_beam)
                logger.info(f"Committed {db_beam}")
            else:
                db_beam = db_beam[0]
                logger.info(f"Found {db_beam}")
            
            beam_index_to_db_id_map[beam_i] = db_beam.id
            beam_index_to_obs_id_map[beam_i] = db_beam.observation_id
        
        logger.debug(f"Beam index to DB BeamID map: {beam_index_to_db_id_map}")
        logger.debug(f"Beam index to Observation ID map: {beam_index_to_obs_id_map}")

        for stamps_filepath in stamps_filepaths:
            for stamp_enum, _stamp in enumerate(seticore_viewer.read_stamps(stamps_filepath)):
                stamp = _stamp.stamp
                session.add(
                    entities.CosmicDB_ObservationStamp(
                        observation_id = beam_index_to_obs_id_map[stamp.signal.beam],
                        tuning = CONTEXT["TUNING"],
                        subband_offset = int(CONTEXT["SCHAN"]),

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

                        signal_frequency = stamp.signal.frequency,
                        signal_index = stamp.signal.index,
                        signal_drift_steps = stamp.signal.driftSteps,
                        signal_drift_rate = stamp.signal.driftRate,
                        signal_snr = stamp.signal.snr if stamp.signal.snr != math.inf else -1.0,
                        signal_coarse_channel = stamp.signal.coarseChannel,
                        signal_beam = stamp.signal.beam,
                        signal_num_timesteps = stamp.signal.numTimesteps,
                        signal_power = stamp.signal.power,
                        signal_incoherent_power = stamp.signal.incoherentPower,

                        beam_id = beam_index_to_db_id_map[stamp.signal.beam],
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
                        subband_offset = int(CONTEXT["SCHAN"]),

                        file_uri = input_to_output_filepath_map[hits_filepath],
                        file_local_enumeration = hit_enum,
                        
                        signal_frequency = hit.signal.frequency,
                        signal_index = hit.signal.index,
                        signal_drift_steps = hit.signal.driftSteps,
                        signal_drift_rate = hit.signal.driftRate,
                        signal_snr = hit.signal.snr if hit.signal.snr != math.inf else -1.0,
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
        common.makedirs(args.destination_dirpath, user="cosmic", group="cosmic", mode=0o777, exist_ok=True)

    all_moved = []
    for inputpath in inputs:
        destinationpath = input_to_output_filepath_map[inputpath]
        cmd = [
            "mv",
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
