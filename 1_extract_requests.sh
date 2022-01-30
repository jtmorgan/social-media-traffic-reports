#!/bin/bash

# activate python3 environment
source /usr/bin/conda-activate-stacked

# make sure web proxy set up so script can reach MediaWiki API
export http_proxy=http://webproxy.eqiad.wmnet:8080
export https_proxy=http://webproxy.eqiad.wmnet:8080

HIVE_TABLE="isaacj.smtr"
LANGS="en"
DATA_THRESHOLD=100
# https://stackoverflow.com/questions/15374752/get-yesterdays-date-in-bash-on-linux-dst-safe
DAY=$(date -d "yesterday 13:00" '+%d')
MONTH=$(date -d "yesterday 13:00" '+%m')
YEAR=$(date -d "yesterday 13:00" '+%Y')

echo "Running pipeline for ${YEAR}-${MONTH}-${DAY}."

python /home/isaacj/social-media-traffic-reports/smtr/extract_requests.py --year "${YEAR}" --month "${MONTH}" --day "${DAY}" --langs "${LANGS}" --hive_table ${HIVE_TABLE} --data_threshold ${DATA_THRESHOLD}

# Only run this automatically after a few days of manual checks for testing
# python /home/isaacj/social-media-traffic-reports/publish_report --data_tsv "${DATA_DIR}/smtr_${YEAR}_${MONTH}_${DAY}_public_watchlist.tsv" --date "${YEAR}-${MONTH}-${DAY}"