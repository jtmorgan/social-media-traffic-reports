#!/bin/bash

# activate python3 environment
source /home/isaacj/venv/bin/activate

# make sure web proxy set up so script can reach MediaWiki API
export http_proxy=http://webproxy.eqiad.wmnet:8080
export https_proxy=http://webproxy.eqiad.wmnet:8080

DATA_DIR="/home/isaacj/social-media-traffic-reports/data"
# https://stackoverflow.com/questions/15374752/get-yesterdays-date-in-bash-on-linux-dst-safe
DAY=$(date -d "yesterday 13:00" '+%d')
MONTH=$(date -d "yesterday 13:00" '+%m')
YEAR=$(date -d "yesterday 13:00" '+%Y')

echo "Running pipeline for ${YEAR}-${MONTH}-${DAY}."

#cmt out because run2.sh only executes the second half of the pipeline
#python /home/isaacj/social-media-traffic-reports/extract_requests.py --year "${YEAR}" --month "${MONTH}" --day "${DAY}" --hive_db ${HIVE_DB} --nice --data_threshold ${DATA_THRESHOLD} --privacy_threshold ${PRIVACY_THRESHOLD} --output_directory ${DATA_DIR}

python publish_report --data_tsv "${DATA_DIR}/smtr_${YEAR}_${MONTH}_${DAY}_public_watchlist.tsv" --date "${YEAR}-${MONTH}-${DAY}"
