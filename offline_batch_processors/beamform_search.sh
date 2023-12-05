#!/bin/bash
set -x

##### test
# /home/cosmic/anaconda3/envs/cosmic_vla/bin/python3 /home/cosmic/anaconda3/envs/cosmic_vla/bin/pypeline 9 hpdaq_rawpart_offline --workers 4 --queue-limit -1 -vvv \
#   -kv "#CONTEXT=hpdaq_rawpart_offline" \
#   "#CONTEXTENV=POSTPROC_PROGRESS_REDIS_CHANNEL=postprocess_progress BATCH_RAWPART_COUNT=7 RAWPART_GLOB_PATTERN=/mnt/cosmic-storage-1/data0/test_batch_process/*.raw" \
#   "#STAGES=bfr5_generate beamform_search dbarchive" \
#   "BFR5GenerateENV=RETROACTIVE_MODE=true" \
#   "BFR5GenerateARG=--telescope-info-toml-filepath /bogus/nonexisitent/failing/telinfo_vla.toml --take-targets 4 --targets-redis-key-timestamp-rawkey PKTSTART" \
#   "BFR5GenerateINP=*hpdaq_rawpart_offline" \
#   "BeamformSearchENV=PATH=$PATH:/home/cosmic/src/blade/install/bin LD_LIBRARY_PATH=$LD_LIBRARY_PATH:/home/cosmic/src/blade/install/lib/:/home/cosmic/src/blade/install/lib/x86_64-linux-gnu" \
#   "BeamformSearchARG=-c 524288 -C 1 -T 4 -N 1 --gpu-shares 2 --gpu-share-index 1 --gpu-target-most-memory --snr-threshold 8.0 --drift-rate-maximum 50.0 --log-blade-output" \
#   "BeamformSearchINP=*hpdaq_rawpart_offline bfr5_generate" \
#   "DBArchiveARG=-c /home/cosmic/conf/cosmicdb_conf.yaml -d /mnt/cosmic-storage-1/data1/batch_processed/$OBSID$" \
#   "DBArchiveINP=bfr5_generate *beamform_search"

/home/cosmic/anaconda3/envs/cosmic_vla/bin/python3 /home/cosmic/anaconda3/envs/cosmic_vla/bin/pypeline 9 hpdaq_rawpart_offline --workers 6 --queue-limit -1 --log-directory /home/cosmic/logs/pypeline_offline/ --log-backup-days 31 -vvv \
  -kv "#CONTEXT=hpdaq_rawpart_offline" \
  "#CONTEXTENV=POSTPROC_PROGRESS_REDIS_CHANNEL=postprocess_progress POSTPROC_MOVE_FAILURES=true BATCH_RAWPART_COUNT=1 RAWPART_GLOB_PATTERN=/mnt/cosmic-storage-1/data1/unprocessed_vlass/*.raw" \
  "#STAGES=mv bfr5_generate beamform_search rm dbarchive" \
  "MoveARG=/mnt/buf0/offline_processing/" \
  "MoveINP=*hpdaq_rawpart_offline" \
  "BFR5GenerateENV=RETROACTIVE_MODE=true" \
  "BFR5GenerateARG=--telescope-info-toml-filepath /bogus/nonexisitent/failing/telinfo_vla.toml --take-targets 5 --targets-redis-key-timestamp-rawkey PKTSTART" \
  "BFR5GenerateINP=*mv" \
  "BeamformSearchENV=PATH=\$PATH:/home/cosmic/src/blade/install/bin LD_LIBRARY_PATH=\$LD_LIBRARY_PATH:/home/cosmic/src/blade/install/lib/:/home/cosmic/src/blade/install/lib/x86_64-linux-gnu" \
  "BeamformSearchARG=-c 131072 -C 1 -T 64 -N 1 --gpu-shares 1 --gpu-share-index 0 --gpu-target-most-memory --snr-threshold 8.0 --drift-rate-maximum 50.0 --log-blade-output" \
  "BeamformSearchINP=*mv bfr5_generate" \
  "RemovalARG="  \
  "RemovalINP=*mv"  \
  "DBArchiveARG=-c /home/cosmic/conf/cosmicdb_conf.yaml -d /mnt/cosmic-storage-1/data1/batch_processed/\$OBSID\$" \
  "DBArchiveINP=bfr5_generate *beamform_search"
