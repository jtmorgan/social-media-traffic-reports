import argparse
import datetime
import os
import time
import traceback

import pandas as pd
import requests
import wmfdata


def valid_args(args):
    """Check if command-line arguments are valid."""
    is_valid = True
    # valid date?
    try:
        datetime.datetime(year=args.year, month=args.month, day=args.day)
    except Exception:
        traceback.print_exc()
        is_valid = False

    # unless another review is conducted by Security, 500 is the lowest allowed privacy threshold
    if args.privacy_threshold < 500:
        print(f"Privacy threshold may be set too low at {args.privacy_threshold}. Should be at least 500.")
        is_valid = False

    # make sure data directory for TSV exists
    if not os.path.isdir(args.output_directory):
        print(f"{args.output_directory} does not exist. You must create it first.")
        is_valid = False

    print("Arguments: {0}".format(args))
    return is_valid

def smtr_counts_to_df(hive_table, year, month, day, lang, privacy_threshold=500):
    """Join pageview data and write traffic counts to TSV for further analysis / publshing."""
    yday = datetime.datetime(year=year, month=month, day=day) - datetime.timedelta(days=1)

    query = f"""
    WITH yesterdays_data AS (
        SELECT
          source,
          page_id,
          sviews_desktop + sviews_mobile AS sviewsyesterday
        FROM {hive_table}
        WHERE
          year = {yday.year} AND month = {yday.month} AND day = {yday.day}
          AND lang = {lang}
          AND (sviews_desktop + sviews_mobile > {privacy_threshold})          
    )
    SELECT
      t.source,
      t.page_id,
      sviews_desktop + sviews_mobile AS sviews,
      COALESCE(sviewsyesterday, 0) AS sviewsyesterday,
      total_views
    FROM {hive_table} t
    LEFT JOIN yesterdays_data y
      ON (t.source = y.source
          AND t.page_id = y.page_id)
    WHERE
      year = {year} AND month = {month} AND day = {day}
      AND lang = {lang}
      AND (sviews_desktop + sviews_mobile > {privacy_threshold})      
    """

    df = wmfdata.hive.run(query, 'pandas')
    return df

def add_metadata(df, output_tsv, lang='en'):
    """Add metadata from API to counts.
    Currently this adds:
    * title: canonical page title for that page ID (may not be the redirect being used)
    * watchers: # of people with article on their watchlist
    * visitingwatchers: # of watchers who visited the page in the last 6 months
    """
    page_ids_sets = chunk(list(set(df['page_id'])), 50)

    base_url = f'https://{lang}.wikipedia.org/w/api.php'
    base_params = {"action":"query",
                   "prop":"info",
                   "format":"json",
                   "formatversion": 2,
                   "inprop": 'watchers|visitingwatchers'}

    watchers = {}
    visitingwatchers = {}
    titles = {}
    with requests.session() as session:
        for pid_set in page_ids_sets:
            params = base_params.copy()
            params['pageids'] = '|'.join(pid_set)
            watchlist_res = session.get(url=base_url, params=params).json()
            for result in watchlist_res['query']['pages']:
                pid = result['pageid']
                watchers[pid] = result.get('watchers', 0)
                visitingwatchers[pid] = result.get('visitingwatchers', 0)
                titles[pid] = result.get('title', '--')
            time.sleep(1)  # be kind to API

    for name, data in {'watchers':watchers, 'visitingwatchers':visitingwatchers, 'page_title':titles}.items():
        metadata = pd.Series(data)
        metadata.name = name
        df = df.join(metadata, how='left', on='page_id')

    df.to_csv(output_tsv, sep='\t', index=False)

def chunk(pageids, batch_size=50):
    """Batch pageIDS into sets of 50 for the Mediawiki API."""
    chunks = []
    for i in range(0, len(pageids), batch_size):
        chunks.append([str(p) for p in pageids[i:i+batch_size]])
    return chunks

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int,
                        help="Year for which to gather data -- e.g., 2020")
    parser.add_argument("--month", type=int,
                        help="Month for which to gather data -- e.g., '3' for March")
    parser.add_argument("--day", type=int,
                        help="Day for which to gather data -- e.g., '15' for 15th of the month")
    parser.add_argument("--lang", default='en', type=str,
                        help="Wikipedia language -- e.g., en for English")
    parser.add_argument("--hive_table", default='isaacj.smtr_by_day',
                        help='Hive database where SMTR table will be stored.')
    parser.add_argument("--privacy_threshold", default=500, type=int,
                        help="Minimum # of externally-referred pageviews to include page in report.")
    parser.add_argument("--output_directory",
                        help="Where to store TSV reports.")
    args = parser.parse_args()

    if valid_args(args):
        # export Hive data to TSV
        print("\n==Joining pageview metadata and pulling locally==")
        raw_counts_df = smtr_counts_to_df(hive_table=args.hive_table,
                                            year=args.year, month=args.month, day=args.day,
                                            lang=args.lang,
                                            privacy_threshold=args.privacy_threshold)

        # add watchlist data
        print("\n==Adding watchlist/title data and outputting to TSV==")
        output_tsv_fn = os.path.join(args.output_directory,
                                     f"smtr_{args.year}_{args.month:02}_{args.day:02}.tsv")
        add_metadata(raw_counts_df, output_tsv=output_tsv_fn, lang=args.lang)
        print("Final TSV at: {0}".format(output_tsv_fn))

if __name__ == "__main__":
    main()