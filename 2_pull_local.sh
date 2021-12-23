#!/bin/bash

# activate python3 environment
source /usr/bin/conda-activate-stacked

# make sure web proxy set up so script can reach MediaWiki API
export http_proxy=http://webproxy.eqiad.wmnet:8080
export https_proxy=http://webproxy.eqiad.wmnet:8080

DATA_DIR="/home/isaacj/social-media-traffic-reports/data"
LANG="en"
PRIVACY_THRESHOLD=500
# https://stackoverflow.com/questions/15374752/get-yesterdays-date-in-bash-on-linux-dst-safe
DAY=$(date -d "yesterday 13:00" '+%d')
MONTH=$(date -d "yesterday 13:00" '+%m')
YEAR=$(date -d "yesterday 13:00" '+%Y')

echo "Running pipeline for ${YEAR}-${MONTH}-${DAY}."

python /home/isaacj/social-media-traffic-reports/pull_data_local.py --year "${YEAR}" --month "${MONTH}" --day "${DAY}" --lang "${LANG}" --hive_table ${HIVE_TABLE} --privacy_threshold ${PRIVACY_THRESHOLD} --output_directory ${DATA_DIR}