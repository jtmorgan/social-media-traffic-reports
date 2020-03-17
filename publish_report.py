#! /usr/bin/env/ python

import argparse
import config #see config.py.sample for more information
from datetime import datetime, timedelta
import pandas as pd
import requests
from requests_oauthlib import OAuth1

#instantiate authentication
auth1 = OAuth1(config.consumer_key,
               config.consumer_secret,
               config.access_key,
               config.access_secret)

#page template (top)
rt_header = """=== Top articles by social media traffic on {year}-{month}-{day} ===
Last updated on ~~~~~

The data in this table is fake, and just used for testing purposes.

{{| class="wikitable sortable"
!Rank
!Platform
!Article
!Platform traffic {month}-{day}
|Platform traffic {month2}-{day2}
|All traffic DATE
|Watchers
|Visiting watchers
"""

#template for individual table rows
rt_row = """|-
|{rank}
|{platform}
|[[w:{title}|{title}]]
|{p_views}
|{p_views_y}
|{t_views}
|{watch}
|{v_watch}
"""

def prepare_data(df_traffic):
    """
    Accept a dataframe with the expected columns
    Returns the full wikipage text as a string
    """

    #sort by descending views
    df_traffic.sort_values('smtpageviews', ascending=False, inplace=True)

    #create a new set of ranks
    new_rank = range(1, len(df_traffic)+1)

    #swap in the new value in the 'Rank' column
    df_traffic['rank'] = list(new_rank)

    #re-zero the index
    df_traffic.reset_index(drop=True, inplace=True)

    #format the individual table rows with the data
    #from each row of the dataframe, preserving the order
    report_rows = [format_row(a, b, c, d, e, f, g, h, rt_row)
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

    date_parts = {'year': datetime.strftime(datetime.now() - timedelta(1), '%Y'),
   'month' : datetime.strftime(datetime.now() - timedelta(1), '%m'),
   'day': datetime.strftime(datetime.now() - timedelta(1), '%d'),
   'month2' : datetime.strftime(datetime.now() - timedelta(2), '%m'),
   'day2': datetime.strftime(datetime.now() - timedelta(2), '%d'),
        }

    header = rt_header.format(**date_parts)

    #combine the header with the now-populated table rows
    output = rt_header + rows_wiki + "|}"

    print(output)
    return(output)


def format_row(rank, platform, title, p_views, p_views_y, t_views, watch, v_watch, row_template):
    """
    Accepts an iterator from the dataframe with data from each row
    Returns the rows formatted per the wikitext table row template
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
        url="https://test.wikipedia.org/w/api.php", #TODO add to config
        params={
            'action': "query",
            'meta': "tokens",
            'type': "csrf",
            'format': "json"
            },
        headers={'User-Agent': "jmorgan@wikimedia.org"}, #TODO add to config
        auth=auth1,
        ).json()

    print(result)

    edit_token = result['query']['tokens']['csrftoken']

    return(edit_token)

def publish_report(output, auth1, edit_token):
    """
    Accepts the page text, credentials and edit token
    Publishes the formatted page text to the specified wiki
    """
    response = requests.post(
    url = "https://test.wikipedia.org/w/api.php", #TODO add to config
    data={
        'action': "edit",
        'title': "User:Jmorgan_(WMF)/sandbox", #TODO add to config
#         'section': "new",
        'summary': "testing publish_report.py", #TODO add to config
        'text': output,
        'bot': 0,
        'token': edit_token,
        'format': "json"
        },
    headers={'User-Agent': "jmorgan@wikimedia.org"}, #TODO add to config
    auth=auth1
        )

    print(response)

if __name__ == "__main__":

    parser = argparse.ArgumentParser()
    parser.add_argument("--data_tsv", default="fake_data.csv",
                        help="TSV file with articles that exceeded the privacy threshold for social-media referrals.")
    args = parser.parse_args()

    df_traffic = pd.read_csv(args.data_tsv)

    output = prepare_data(df_traffic)

#     edit_token = get_token(auth1)
#
#     publish_report(output, auth1, edit_token)



