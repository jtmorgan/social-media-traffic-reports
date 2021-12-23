import argparse
import datetime
import traceback

import wmfdata

import config  # see config.py.sample for more information

def start_session(user_agent):
    spark = wmfdata.spark.get_session(app_name=f'pyspark reg; smtr; {user_agent}',
                                      type='yarn-regular',  # local, yarn-regular, yarn-large
                                      )
    return spark

def valid_args(args):
    """Check if command-line arguments are valid."""
    is_valid = True

    # valid date format?
    try:
        datetime.datetime(year=args.year, month=args.month, day=args.day)
    except Exception:
        traceback.print_exc()
        is_valid = False

    print(f"Arguments: {args}")
    return is_valid


def create_hive_trace_table(spark, table='isaacj.smtr'):
    """Create a table for pageview counts partitioned by day."""

    query = f"""
    CREATE TABLE IF NOT EXISTS {table} (
        source         STRING COMMENT 'Cleaned external referer source',
        lang           STRING COMMENT 'Wikipedia language edition -- e.g., en for English',
        page_id        INT    COMMENT 'Page ID of article',
        sviews_desktop INT    COMMENT 'Number of desktop pageviews from source',
        sviews_mobile  INT    COMMENT 'Number of mobile pageviews from source',
        total_views    INT    COMMENT 'Total views from all sources'
    )
    PARTITIONED BY (year INT, month INT, day INT)
    STORED AS PARQUET
    """

    print(query)
    spark.sql(query)


def add_day_to_hive_smtr_table(spark, year, month, day, data_threshold=100, langs=('en',), table='isaacj.smtr'):
    """Add one day's worth of data to the Hive table."""
    wikis = "('" + "','".join([f'{l}.wikipedia' for l in langs]) + "')"
    langs = "('" + "','".join(langs) + "')"

    query = f"""
    WITH potential_referrals AS (
        SELECT
          host_to_site(PARSE_URL(referer, 'HOST')) AS source,
          normalized_host.project AS lang,
          page_id,
          access_method
        FROM wmf.pageview_actor
        WHERE
          year = {year} AND month = {month} AND day = {day}
          AND is_pageview
          AND normalized_host.project_class = 'wikipedia'
          AND normalized_host.project IN {langs}
          AND namespace_id = 0
          AND agent_type = 'user'
          AND referer_class = 'external'
          AND (referer LIKE '%reddit.com%' OR
               referer LIKE '%facebook.com%' OR 
               referer LIKE '%t.co/%' OR 
               referer LIKE '%twitter.com%' OR 
               referer LIKE '%youtu%')
    ),
    social_media_referrals AS (
        SELECT
          source,
          lang,
          page_id,
          SUM(IF(access_method = 'desktop', 1, 0)) as sviews_desktop,
          SUM(IF(access_method = 'mobile web', 1, 0)) as sviews_mobile
        FROM potential_referrals
        WHERE
          source IS NOT NULL
        GROUP BY
          source,
          lang,
          page_id
        HAVING COUNT(1) > {data_threshold}        
    ),
    distinct_pages AS (
        SELECT DISTINCT
          lang,
          page_id
        FROM social_media_referrals
    ),
    total_pageviews AS (
        SELECT
          s.lang AS lang,
          p.page_id AS page_id,
          SUM(view_count) AS total_views
        FROM wmf.pageview_hourly p
        INNER JOIN distinct_pages s
          ON (p.project = CONCAT(s.lang, '.wikipedia') 
              AND p.page_id = s.page_id)
        WHERE
          year = {year} AND month = {month} AND day = {day}
          AND namespace_id = 0
          AND agent_type = 'user'
          AND project IN {wikis}
        GROUP BY
          s.lang,
          p.page_id
    )
    INSERT OVERWRITE TABLE {table}
    PARTITION(year={year}, month={month}, day={day})
    SELECT
      source,
      s.lang,
      s.page_id,
      sviews_desktop,
      sviews_mobile,
      total_views
    FROM social_media_referrals s
    LEFT JOIN total_pageviews t
        ON (s.lang = t.lang
            AND s.page_id = t.page_id)"""

    print(query)
    spark.sql(query)

def host_to_site(host):
    """Aggregate URL hosts to more general platforms."""

    if host:
        # www.facebook.com m.facebook.com l.facebook.com lm.facebook.com
        if host.endswith('facebook.com'):
            return 'Facebook'
        # youtu.be www.youtube.com youtube.com m.youtube.com
        elif host.endswith('youtube.com') or host == 'youtu.be':
            return 'YouTube'
        # old.reddit.com www.reddit.com
        elif host.endswith('reddit.com'):
            return 'Reddit'
        # t.co twitter.com
        elif host.endswith('twitter.com') or host == 't.co':
            return 'Twitter'
        elif host.endswith('tikitok.com'):
            return 'TikTok'
    return None


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int,
                        help="Year for which to gather data -- e.g., 2020")
    parser.add_argument("--month", type=int,
                        help="Month for which to gather data -- e.g., '3' for March")
    parser.add_argument("--day", type=int,
                        help="Day for which to gather data -- e.g., '15' for 15th of the month")
    parser.add_argument("--langs", default='en', nargs="*", type=str,
                        help="Wikipedia languages -- e.g., en for English")
    parser.add_argument("--hive_table", default='isaacj.smtr_by_day',
                        help='Hive table where SMTR data will be stored.')
    parser.add_argument("--data_threshold", default=20, type=int,
                        help="Minimum # of externally-referred pageviews to retain page in Hive tables.")
    args = parser.parse_args()

    if valid_args(args):
        # setup pyspark session
        spark = start_session(config.user_agent)
        spark.udf.register('host_to_site', host_to_site, 'String')

        # Create table in Hive to store data
        print("\n==Creating Hive table (if it doesn't exist)==")
        create_hive_trace_table(spark=spark, table=args.hive_table)

        # Add day to table
        print(f"\n==Adding data from {args.year}-{args.month}-{args.day}==")
        add_day_to_hive_smtr_table(spark=spark,
                                   year=args.year, month=args.month, day=args.day,
                                   langs=args.langs,
                                   data_threshold=args.data_threshold,
                                   table=args.hive_table)

if __name__ == "__main__":
    main()
