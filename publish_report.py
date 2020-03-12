#! /usr/bin/env/ python

import config #see config.py.sample for more information
import pandas as pd
import requests
from requests_oauthlib import OAuth1

#instantiate authentication
auth1 = OAuth1(config.consumer_key, 
               config.consumer_secret, 
               config.access_key, 
               config.access_secret)

#page template (top)
rt_header = """=== Top articles by social media traffic ===
Last updated on ~~~~~

The data in this table is fake, and just used for testing purposes.

{| class="wikitable sortable"
!Rank
!Platform
!Page title
!Page views
"""

#template for individual table rows
rt_row = """|-
|{rank}
|{platform}
|[[w:{title}|{title}]]
|{views}
"""

def prepare_data(df_traffic):
    """
    Accept a dataframe with the expected columns
    Returns the full wikipage text as a string
    """

    #sort by descending views
    df_traffic.sort_values('Page_views', ascending=False, inplace=True)

    #create a new set of ranks
    new_rank = range(1, len(df_traffic)+1)

    #swap in the new value in the 'Rank' column
    df_traffic['Rank'] = list(new_rank)

    #re-zero the index
    df_traffic.reset_index(drop=True, inplace=True)
    
    #format the individual table rows with the data
    #from each row of the dataframe, preserving the order
    report_rows = [format_row(x, y, z, a, rt_row) for x, y, z, a in zip(df_traffic['Rank'], df_traffic['Platform'], df_traffic['Page_Title'], df_traffic['Page_views'])]

    #join this list into a single string
    rows_wiki = ''.join(report_rows)
    
    #combine the header with the now-populated table rows
    output = rt_header + rows_wiki + "|}"
    
    print(output)
    return(output)
    
    
def format_row(rank, platform, title, views, row_template):
    """
    Accepts an iterator from the dataframe with data from each row
    Returns the rows formatted per the wikitext table row template
    """
    
    table_row = {'rank': rank, 
           'platform' : platform, 
           'title': title, 
        'views' : views,
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
        'summary': "testing publish_report.py using fake data", #TODO add to config
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
    
    df_traffic = pd.read_csv("fake_data.csv") #TODO pass file title as command line arg

    output = prepare_data(df_traffic)

    edit_token = get_token(auth1)
    
    publish_report(output, auth1, edit_token)



