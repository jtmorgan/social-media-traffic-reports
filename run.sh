#!/bin/bash

HIVE_DB="isaacj"
DATA_THRESHOLD=20
PRIVACY_THRESHOLD=500
DATA_DIR="./data"
# https://stackoverflow.com/questions/15374752/get-yesterdays-date-in-bash-on-linux-dst-safe
DAY=$(date -d "yesterday 13:00" '+%d')
MONTH=$(date -d "yesterday 13:00" '+%m')
YEAR=$(date -d "yesterday 13:00" '+%Y')

echo "Running pipeline for ${YEAR}-${MONTH}-${DAY}."

python extract_requests.py --year "${YEAR}" --month "${MONTH}" --day "${DAY}" --hive_db ${HIVE_DB} --nice --data_threshold ${DATA_THRESHOLD} --privacy_threshold ${PRIVACY_THRESHOLD} --output_directory ${DATA_DIR}

# Only run this automatically after a few days of manual checks for testing
# python publish_report --data_tsv "${DATA_DIR}/smtr_${YEAR}_${MONTH}_${DAY}_public_watchlist.tsv"
