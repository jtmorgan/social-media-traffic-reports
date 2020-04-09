#! /usr/bin/env/ python

import argparse
import config #see config.py.sample for more information
from datetime import datetime, timedelta
import pandas as pd
import requests
from requests_oauthlib import OAuth1

##TODO
#percent change d-o-d column
#>30, >100? instaed of zeros
#argparse switch to print to either sandbox or production

#instantiate authentication
auth1 = OAuth1(config.consumer_key,
               config.consumer_secret,
               config.access_key,
               config.access_secret)

#page sectiona and wikitable template (top)
RT_HEADER = """== Top articles by social media traffic on {year}-{month}-{day} ==
Last updated on ~~~~~

{{| class="wikitable sortable"
!Rank
!Platform
!Article
!Platform traffic {month}-{day}
!Platform traffic {month2}-{day2}
!All traffic {month}-{day}
!Watchers
!Visiting watchers
"""

#template for individual table rows
RT_ROW = """|-
|{rank}
|{platform}
|[[w:{title}|{title}]]
|{p_views}
|{p_views_y}
|{t_views}
|{watch}
|{v_watch}
"""

def prepare_data(df_traffic, data_date=None):
    """
    Accept a dataframe with the expected columns
    Returns the full wikipage text as a string
    """

    #sort dataframe by views this platform yesterday (descending)
    df_traffic.sort_values('smtpageviews', ascending=False, inplace=True)

    #rank rows by views from this platform yesterday
    new_rank = range(1, len(df_traffic) + 1)
    df_traffic['rank'] = list(new_rank)

    #reset the index to reflect the ranking
    df_traffic.reset_index(drop=True, inplace=True)
    
    df_traffic = format_lower_limits(df_traffic)

    #format the wikitable rows with relevant columns from the dataframe
    report_rows = [format_row(a, b, c, d, e, f, g, h, RT_ROW)
    for a, b, c, d, e, f, g, h
    in zip(df_traffic['rank'],
            df_traffic['site'],
            df_traffic['page_title'],
            df_traffic['smtpageviews'],
            df_traffic['smtcountyesterday'],
            df_traffic['totalpageviews'],
            df_traffic['watchers'],
            df_traffic['visitingwatchers'],
            )]

    #join this list into a single string
    rows_wiki = ''.join(report_rows)

    #get datestrings from yesterday and the day before to populate the wikitable
    date_parts = get_yesterdates(data_date)

    #format wikipage header template
    header = RT_HEADER.format(**date_parts)

    #combine the header with the now-populated table rows
    output = header + rows_wiki + "|}"

#     print(output)
    return(output)

def format_lower_limits(df_traffic): #need to refactor!
    """
    Accepts a dataframe with these specific columns
    Returns a dataframe with lower counts reformatted to more user-friendly values
    """
    df_traffic.loc[(df_traffic.smtcountyesterday == 0), "smtcountyesterday"] = "< 500"
    df_traffic.loc[(df_traffic.visitingwatchers == 0), "visitingwatchers"] = "< 30"
    df_traffic.loc[(df_traffic.watchers == 0), "watchers"] = "< 30"

    return df_traffic

def get_yesterdates(data_date=None):
    """
    Returns month, day year for yesterday; month and day for day before
    """
    if data_date is None:
        # assume data is from yesterday
        date_parts = {'year': datetime.strftime(datetime.now() - timedelta(1), '%Y'),
                      'month' : datetime.strftime(datetime.now() - timedelta(1), '%m'),
                      'day': datetime.strftime(datetime.now() - timedelta(1), '%d'),
                      'month2' : datetime.strftime(datetime.now() - timedelta(2), '%m'),
                      'day2': datetime.strftime(datetime.now() - timedelta(2), '%d'),
                      }
    else:
        data_date = datetime.strptime(data_date, '%Y-%m-%d')
        date_parts = {'year': datetime.strftime(data_date, '%Y'),
                      'month': datetime.strftime(data_date, '%m'),
                      'day': datetime.strftime(data_date, '%d'),
                      'month2': datetime.strftime(data_date - timedelta(1), '%m'),
                      'day2': datetime.strftime(data_date - timedelta(1), '%d'),
                      }

    return date_parts

def format_row(rank, platform, title, p_views, p_views_y, t_views, watch, v_watch, row_template):
    """
    Accepts column values from the dataframe with data from each row
    Returns a row formatted per the wikitext table row template
    """

    table_row = {'rank': rank,
           'platform' : platform,
           'title': title,
           'p_views' : p_views,
           'p_views_y' : p_views_y,
           't_views' : t_views,
           'watch' : watch,
           'v_watch' :v_watch,
                    }

    row = row_template.format(**table_row)
#     print(row)
    return(row)

def get_token(auth1):
    """
    Accepts an auth object for a user
    Returns an edit token for the specified wiki
    """

    result = requests.get(
        url = config.url,
        params={
            'action': "query",
            'meta': "tokens",
            'type': "csrf",
            'format': "json",
            },
        headers={'User-Agent': config.user_agent},
        auth=auth1,
        ).json()

#     print(result)
    edit_token = result['query']['tokens']['csrftoken']

    return(edit_token)

def publish_report(output, auth1, edit_token):
    """
    Accepts the page text, credentials and edit token
    Publishes the formatted page text to the specified wiki
    """

    edit_sum = "Social media traffic report for {year}-{month}-{day}".format(**get_yesterdates()) #untested

    response = requests.post(
    url = config.url,
    data={
        'action': "edit",
        'title': config.page_title,
        'section': config.edit_sec,
        'summary': edit_sum,
        'text': output,
        'bot': 0,
        'token': edit_token,
        'format': "json",
        },
    headers={'User-Agent': config.user_agent},
    auth=auth1
        )
    print("Successfully posted: {0}".format(edit_sum))

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--data_tsv",
                        help="TSV file with articles that exceeded the privacy threshold for social-media referrals.")
    parser.add_argument("--date",
                        default=datetime.strftime(datetime.now() - timedelta(1), "%Y-%m-%d"),
                        help="Date of data in YYYY-MM-DD format")
    args = parser.parse_args()

    df_traffic = pd.read_csv(args.data_tsv, delimiter='\t', encoding='utf-8')

    output = prepare_data(df_traffic, args.date)

    edit_token = get_token(auth1)

    publish_report(output, auth1, edit_token)



