#!/bin/bash

# activate python3 environment
source /usr/bin/conda-activate-stacked

# make sure web proxy set up so script can reach MediaWiki API
export http_proxy=http://webproxy.eqiad.wmnet:8080
export https_proxy=http://webproxy.eqiad.wmnet:8080

DATA_DIR="/home/isaacj/social-media-traffic-reports/data"
# https://stackoverflow.com/questions/15374752/get-yesterdays-date-in-bash-on-linux-dst-safe
DAY=$(date -d "yesterday 13:00" '+%d')
MONTH=$(date -d "yesterday 13:00" '+%m')
YEAR=$(date -d "yesterday 13:00" '+%Y')

echo "Running pipeline for ${YEAR}-${MONTH}-${DAY}."

python /home/isaacj/social-media-traffic-reports/smtr/publish_report.py --data_tsv "${DATA_DIR}/smtr_${YEAR}_${MONTH}_${DAY}.tsv" --date "${YEAR}-${MONTH}-${DAY}"