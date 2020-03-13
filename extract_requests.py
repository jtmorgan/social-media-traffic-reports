import argparse
import datetime
import os
import requests
import subprocess
import time
import traceback

import pandas as pd

def exec_hive_stat2(query, filename=None, verbose=True, nice=True, large=False):
    """Query Hive."""
    if nice:
        query = "SET mapreduce.job.queuename=nice; " + query
    if large:
        query = "SET mapreduce.map.memory.mb=4096; " + query
    cmd = """hive -e \" """ + query + """ \""""
    if filename:
        cmd = cmd + " > " + filename
    if verbose:
        print(cmd)
    ret = os.system(cmd)
    return ret


def create_hive_trace_table(hive_db='isaacj', nice=True):
    """
    Create a table for pageview counts partitioned by day
    """

    query = """
    CREATE TABLE IF NOT EXISTS {0}.smtr_by_day (
        RefererHost STRING,
        PageID INT,
        CountExtDesktop INT,
        CountExtMobile INT,
        CountExtAll INT)
    PARTITIONED BY (year INT, month INT, day INT)
    ROW FORMAT DELIMITED
    FIELDS TERMINATED BY '\t'
    STORED AS PARQUET;
    """.format(hive_db)

    # PageTitle STRING,

    exec_hive_stat2(query, nice=nice)


def add_day_to_hive_smtr_table(year, month, day, data_threshold=100, hive_db='isaacj', nice=True):
    query = """
    INSERT OVERWRITE TABLE {0}.smtr_by_day
    PARTITION(year={1}, month={2}, day={3})
    SELECT PARSE_URL(referer, 'HOST') AS RefererHost,
           page_id as PageID,
           SUM(IF(access_method = 'desktop', 1, 0)) as CountExtDesktop,
           SUM(IF(access_method = 'mobile web', 1, 0)) as CountExtMobile,
           COUNT(1) AS CountExtAll
      FROM wmf.webrequest
     WHERE year = {1} AND month = {2} AND day = {3}
           AND is_pageview
           AND normalized_host.project_class = 'wikipedia'
           AND normalized_host.project = 'en'
           AND namespace_id = 0
           AND agent_type = 'user'
           AND (referer LIKE '%reddit.com' OR
                referer LIKE '%facebook.com' OR 
                referer LIKE '%t.co/%' OR 
                referer LIKE '%twitter.com' OR 
                referer LIKE '%youtu%')
     GROUP BY page_id,
           PARSE_URL(referer, 'HOST')
    HAVING COUNT(1) > {4};""".format(hive_db, year, month, day, data_threshold)

    # pageview_info['page_title'] AS PageTitle,  # add to GROUP BY as well

    exec_hive_stat2(query, nice=nice)


def smtr_counts_to_tsv(hive_db, year, month, day, output_dir):
    # Embarrassingly I can't tell you why I need to use "MAX" in this query.
    # I thought "SUM" was the appropriate aggregator but that resulted in weird behavior.
    yesterday = datetime.datetime(year=year, month=month, day=day) - datetime.timedelta(days=1)
    query = """
    SELECT t.*,
           COALESCE(y.CountExtAll, 0) AS SMTCountYesterday
      FROM (
            SELECT s.RefererHost,
                   s.PageID,
                   MAX(s.CountExtDesktop) AS CountExtDesktop,
                   MAX(s.CountExtMobile) AS CountExtMobile,
                   MAX(s.CountExtAll) AS CountExtAll,
                   SUM(p.view_count) AS TotalPageViews
              FROM {0}.smtr_by_day s
                   LEFT OUTER JOIN wmf.pageview_hourly p
                   ON (s.PageID = p.page_id
                       AND p.year = {1} AND p.month = {2} AND p.day = {3}
                       AND p.namespace_id = 0
                       AND p.agent_type = 'user'
                       AND p.project = 'en.wikipedia')
             WHERE s.year = {1} AND s.month = {2} AND s.day = {3}
             GROUP BY s.RefererHost, s.PageID
            ) t
      LEFT OUTER JOIN (
                       SELECT pageID,
                              SUM(CountExtAll) AS CountExtAll
                         FROM isaacj.smtr_by_day
                        WHERE year = {4} AND month = {5} AND day = {6}
                        GROUP BY pageID
          ) y
        ON (t.pageID = y.pageID);""".format(hive_db, year, month, day, yesterday.year, yesterday.month, yesterday.day)

    output_tsv_fn = os.path.join(output_dir, "smtr_{0}_{1}_{2}.tsv".format(year, month, day))
    exec_hive_stat2(query, output_tsv_fn)
    return output_tsv_fn

def add_metadata(tsv):
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
            time.sleep(1)

    for name, data in {'watchers':watchers, 'visitingwatchers':visitingwatchers, 'page_title':titles}.items():
        metadata = pd.Series(data)
        metadata.name = name
        df = df.join(metadata, how='left', on='pageid')

    output_tsv_fn = tsv.replace('.tsv', '_watchlist.tsv')
    df.to_csv(output_tsv_fn, sep='\t', index=False)
    return output_tsv_fn


def chunk(pageids, batch_size=50):
    chunks = []
    for i in range(0, len(pageids), batch_size):
        chunks.append([str(p) for p in pageids[i:i+batch_size]])
    return chunks


def make_public(tsv, privacy_threshold=500):
    df = pd.read_csv(tsv, sep='\t')
    print("{0} rows before aggregation / trimming.".format(len(df)))
    df['site'] = df['refererhost'].apply(host_to_site)
    df = df[~df['site'].isnull()]
    df = df.groupby(['site', 'pageid'])[['countextdesktop', 'countextmobile', 'countextall', 'smtcountyesterday', 'totalpageviews']].sum()
    df = df[df['countextall'] > privacy_threshold]
    df['smtcountyesterday'] = df['smtcountyesterday'].apply(enforce_threshold, args=(privacy_threshold,))
    output_tsv_fn = tsv.replace('.tsv', '_public.tsv')
    df.to_csv(output_tsv_fn, sep='\t')
    print("{0} rows after aggregation / trimming.".format(len(df)))
    return output_tsv_fn

def enforce_threshold(val, threshold):
    if val >= threshold:
        return val
    else:
        return 0

def host_to_site(host):
    if 'facebook' in host:
        return 'Facebook'
    elif 'youtu' in host:
        return 'Youtube'
    elif 'reddit' in host:
        return 'Reddit'
    elif 'twitter' in host or 't.co' in host:
        return 'Twitter'

def valid_args(args):
    is_valid = True
    try:
        datetime.datetime(year=args.year, month=args.month, day=args.day)
    except Exception:
        traceback.print_exc()
        is_valid = False

    try:
        check_kerberos_auth()
    except OSError:
        traceback.print_exc()
        is_valid = False

    if args.data_threshold > args.privacy_threshold:
        print("Data threshold is {0}, which is greater than the privacy threshold ({1}).".format(args.data_threshold, args.privacy_threshold))
        is_valid = False

    if args.privacy_threshold < 500:
        print("WARNING: privacy threshold may be set too low at {0}. Should be at least 500.".format(args.privacy_threshold))

    if not os.path.exists(args.output_directory):
        print("{0} does not exist. You must create it first.".format(args.output_directory))
        is_valid = False

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

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, help="Year for which to gather data -- e.g., 2020")
    parser.add_argument("--month", type=int, help="Month for which to gather data -- e.g., '3' for March")
    parser.add_argument("--day", type=int, help="Day for which to gather data -- e.g., '15' for 15th of the month")
    parser.add_argument("--hive_db", default='isaacj', help='Hive database where SMTR table will be stored.')
    parser.add_argument("--nice", default=True, action='store_false', help='Run queries as low priority.')
    parser.add_argument("--data_threshold", default=100, type=int, help="Minimum # of externally-referred pageviews to retain page in Hive tables.")
    parser.add_argument("--privacy_threshold", default=500, type=int, help="Minimum # of externally-referred pageviews to include page in report.")
    parser.add_argument("--output_directory", help="Where to store TSV reports.")
    args = parser.parse_args()

    if valid_args(args):
        # Create table in Hive to store data
        print("\n==Creating Hive table (if it doesn't exist)==")
        create_hive_trace_table(hive_db=args.hive_db,
                                nice=args.nice)

        # Add day to table
        print("\n==Adding data from {0}-{1}-{2}==".format(args.year, args.month, args.day))
        add_day_to_hive_smtr_table(year=args.year, month=args.month, day=args.day,
                                   data_threshold=args.data_threshold,
                                   hive_db=args.hive_db,
                                   nice=args.nice)

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
        metadata_tsv = add_metadata(public_tsv)
        print("Counts + watchlist data TSV at: {0}".format(metadata_tsv))


if __name__ == "__main__":
    main()