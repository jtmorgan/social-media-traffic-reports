import argparse
import datetime
import os
import subprocess
import time
import traceback

import pandas as pd
import requests


def valid_args(args):
    """Check if command-line arguments are valid."""
    is_valid = True
    # valid date?
    try:
        datetime.datetime(year=args.year, month=args.month, day=args.day)
    except Exception:
        traceback.print_exc()
        is_valid = False

    # have run kinit?
    try:
        check_kerberos_auth()
    except OSError:
        traceback.print_exc()
        is_valid = False

    # data threshold (private) should be less than or equal to privacy threshold (public)
    if args.data_threshold > args.privacy_threshold:
        print("Data threshold is {0}, which is greater than the privacy threshold ({1}).".format(args.data_threshold, args.privacy_threshold))
        is_valid = False

    # unless another review is conducted by Security, 500 is the lowest allowed privacy threshold
    if args.privacy_threshold < 500:
        print("Privacy threshold may be set too low at {0}. Should be at least 500.".format(args.privacy_threshold))
        is_valid = False

    # make sure data directory for TSV exists
    if not os.path.isdir(args.output_directory):
        print("{0} does not exist. You must create it first.".format(args.output_directory))
        is_valid = False

    print("Arguments: {0}".format(args))
    return is_valid

def check_kerberos_auth():
    """Check Kerberos authentication.

    Taken from https://github.com/neilpquinn/wmfdata/blob/a2d0ed1085ccc6cf79e74f4b8da6dd1f72eef1f9/wmfdata/utils.py
    TODO: depend on wmfdata package.
    """
    klist = subprocess.call(["klist", "-s"])
    if klist == 1:
        raise OSError(
            "You do not have Kerberos credentials. Authenticate using `kinit` "
            "or run your script as a keytab-enabled user."
        )
    elif klist != 0:
        raise OSError(
          "There was an unknown issue checking your Kerberos credentials."
        )

def exec_hive_stat2(query, filename=None, verbose=True, large=False):
    """Query Hive."""
    if large:
        query = "SET mapreduce.map.memory.mb=4096; " + query
    cmd = """hive -e \" """ + query + """ \""""
    if filename:
        cmd = cmd + " > " + filename
    if verbose:
        print(cmd)
    ret = os.system(cmd)
    return ret

def create_hive_trace_table(hive_db='isaacj'):
    """Create a table for pageview counts partitioned by day."""

    query = """
    CREATE TABLE IF NOT EXISTS {0}.smtr_by_day (
        RefererHost STRING,
        PageID INT,
        SMTPVDesktop INT,
        SMTPVMobile INT,
        SMTPageViews INT)
    PARTITIONED BY (year INT, month INT, day INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS PARQUET;
    """.format(hive_db)

    exec_hive_stat2(query)


def add_day_to_hive_smtr_table(year, month, day, data_threshold=100, hive_db='isaacj'):
    """Add one day's worth of data to the Hive table."""

    query = """
    INSERT OVERWRITE TABLE {0}.smtr_by_day
    PARTITION(year={1}, month={2}, day={3})
    SELECT PARSE_URL(referer, 'HOST') AS RefererHost,
           page_id as PageID,
           SUM(IF(access_method = 'desktop', 1, 0)) as SMTPVDesktop,
           SUM(IF(access_method = 'mobile web', 1, 0)) as SMTPVMobile,
           COUNT(1) AS SMTPageViews
      FROM wmf.pageview_actor
     WHERE year = {1} AND month = {2} AND day = {3}
           AND is_pageview
           AND normalized_host.project_class = 'wikipedia'
           AND normalized_host.project = 'en'
           AND namespace_id = 0
           AND agent_type = 'user'
           AND (referer LIKE '%reddit.com%' OR
                referer LIKE '%facebook.com%' OR 
                referer LIKE '%t.co/%' OR 
                referer LIKE '%twitter.com%' OR 
                referer LIKE '%youtu%')
     GROUP BY page_id,
           PARSE_URL(referer, 'HOST')
    HAVING COUNT(1) > {4};""".format(hive_db, year, month, day, data_threshold)

    exec_hive_stat2(query)


def smtr_counts_to_tsv(hive_db, year, month, day, output_dir):
    """Write traffic counts to TSV for further analysis / publshing.

    NOTE: Embarrassingly I can't tell you why I need to use "MAX" in this query.
    I thought "SUM" was the appropriate aggregator but that resulted in weird behavior
    even though each RefererHost and pageID should only occur once in a given partition.
    """
    query = """
    SELECT s.RefererHost,
           s.PageID,
           MAX(s.SMTPageViews) AS SMTPageViews,
           SUM(p.view_count) AS TotalPageViews
      FROM {0}.smtr_by_day s
      LEFT OUTER JOIN wmf.pageview_hourly p
           ON (s.PageID = p.page_id
               AND p.year = {1} AND p.month = {2} AND p.day = {3}
               AND p.namespace_id = 0
               AND p.agent_type = 'user'
               AND p.project = 'en.wikipedia')
     WHERE s.year = {1} AND s.month = {2} AND s.day = {3}
     GROUP BY s.RefererHost, s.PageID;""".format(hive_db, year, month, day)

    output_tsv_fn = os.path.join(output_dir, "smtr_{0}_{1:02}_{2:02}.tsv".format(year, month, day))
    exec_hive_stat2(query, output_tsv_fn)
    return output_tsv_fn

def make_public(tsv, privacy_threshold=500):
    """Enforce privacy thresholds for public reporting."""
    df = pd.read_csv(tsv, sep='\t')
    print("{0} rows before aggregation / trimming.".format(len(df)))
    df = df[~df['refererhost'].isnull()]
    print("{0} rows after removing null hosts.".format(len(df)))
    df['site'] = df['refererhost'].apply(host_to_site)
    df = df[~df['site'].isnull()]
    print("{0} rows after removing false positive sites.".format(len(df)))
    df = df.groupby(['site', 'pageid'])[['smtpageviews', 'totalpageviews']].agg({'smtpageviews':sum, 'totalpageviews':max})
    print("{0} rows after grouping by platform.".format(len(df)))
    df = df[df['smtpageviews'] > privacy_threshold]
    print("{0} rows after enforcing privacy threshold of {1} page views.".format(len(df), privacy_threshold))
    output_tsv_fn = tsv.replace('.tsv', '_public.tsv')
    df.to_csv(output_tsv_fn, sep='\t')
    return output_tsv_fn

def host_to_site(host):
    """Aggregate URL hosts to more general platforms."""

    # www.facebook.com m.facebook.com l.facebook.com lm.facebook.com
    if 'facebook' in host:
        return 'Facebook'
    # youtu.be www.youtube.com youtube.com m.youtube.com
    elif 'youtu' in host:
        return 'Youtube'
    # old.reddit.com www.reddit.com
    elif 'reddit' in host:
        return 'Reddit'
    # t.co twitter.com
    elif 'twitter' in host or host == 't.co':
        return 'Twitter'

def add_metadata(tsv, yesterday_tsv=None):
    """Add metadata from API to counts.

    Currently this adds:
    * title: canonical page title for that page ID (may not be the redirect being used)
    * watchers: # of people with article on their watchlist
    * visitingwatchers: # of watchers who visited the page in the last 6 months
    * smtcountyesterday: # of pageviews from the same site from yesterday's report
    """
    df = pd.read_csv(tsv, sep='\t')
    page_ids_sets = chunk(list(set(df['pageid'])), 50)

    base_url = 'https://en.wikipedia.org/w/api.php'
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
        df = df.join(metadata, how='left', on='pageid')

    # add social media traffic counts from yesterdays report.
    # set to zero if no report or site-pageID not in yesterday's report
    if os.path.exists(yesterday_tsv):
        yday = pd.read_csv(yesterday_tsv, sep='\t')
        yday['site_pageid'] = yday.apply(lambda x: '{0}-{1}'.format(x['site'], x['pageid']), axis=1)
        yday.set_index('site_pageid', inplace=True)
        yday = yday['smtpageviews'].to_dict()
        df['smtcountyesterday'] = df.apply(match_yesterday, args=(yday,), axis=1)
    else:
        print("Did not find data from yesterday: {0}".format(yesterday_tsv))
        df['smtcountyesterday'] = 0

    output_tsv_fn = tsv.replace('.tsv', '_watchlist.tsv')
    df.to_csv(output_tsv_fn, sep='\t', index=False)
    return output_tsv_fn

def match_yesterday(row, yesterdays_data):
    """Look up unique site-pageID in yesterday's traffic data."""
    uid = '{0}-{1}'.format(row['site'], row['pageid'])
    return yesterdays_data.get(uid, 0)


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
    parser.add_argument("--hive_db", default='isaacj',
                        help='Hive database where SMTR table will be stored.')
    parser.add_argument("--data_threshold", default=20, type=int,
                        help="Minimum # of externally-referred pageviews to retain page in Hive tables.")
    parser.add_argument("--privacy_threshold", default=500, type=int,
                        help="Minimum # of externally-referred pageviews to include page in report.")
    parser.add_argument("--output_directory",
                        help="Where to store TSV reports.")
    args = parser.parse_args()

    if valid_args(args):
        # Create table in Hive to store data
        print("\n==Creating Hive table (if it doesn't exist)==")
        create_hive_trace_table(hive_db=args.hive_db)

        # Add day to table
        print("\n==Adding data from {0}-{1}-{2}==".format(args.year, args.month, args.day))
        add_day_to_hive_smtr_table(year=args.year, month=args.month, day=args.day,
                                   data_threshold=args.data_threshold,
                                   hive_db=args.hive_db)

        # export Hive data to TSV
        print("\n==Joining pageview_hourly and outputting to TSV==")
        raw_counts_tsv = smtr_counts_to_tsv(hive_db=args.hive_db,
                                            year=args.year, month=args.month, day=args.day,
                                            output_dir=args.output_directory)
        print("Raw counts TSV at: {0}".format(raw_counts_tsv))

        # clean up
        print("\n==Cleaning / applying privacy thresholds==")
        public_tsv = make_public(raw_counts_tsv, privacy_threshold=args.privacy_threshold)
        print("Aggregated TSV at: {0}".format(public_tsv))

        # add watchlist data
        print("\n==Adding watchlist/title data==")
        yday = datetime.datetime(year=args.year, month=args.month, day=args.day) - datetime.timedelta(days=1)
        yesterday_tsv = os.path.join(args.output_directory,
                                     "smtr_{0}_{1:02}_{2:02}_public.tsv".format(yday.year, yday.month, yday.day))
        metadata_tsv = add_metadata(public_tsv, yesterday_tsv=yesterday_tsv)
        print("Counts + watchlist data TSV at: {0}".format(metadata_tsv))


if __name__ == "__main__":
    main()