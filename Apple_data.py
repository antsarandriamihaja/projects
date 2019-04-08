import ondemand
import csv
import twitter
import datetime
import json
import ndjson
import os
import argparse
import logging

from google.cloud.exceptions import NotFound
import google.cloud.bigquery as bq

### RETRIEVING DATA

def get_financial_data(company, delay, destPath, barchartKey):

    """Get {company}'s last {delay} days of stock data from barchart using {barchartKey}. The data will be saved as a csv file in {destPath}.
    Args:
        company (str) [REQUIRED]:
            The symbol for the company you wish to get stock data for.
        delay (int) [OPTIONAL]:
            Number of days from today's date you wish to get stock data for. Default is 30 days.
        destPath (str) [OPTIONAL]:
            file path to save data in .csv format. Default is "financial_data.csv"
        barchartKey (str) [REQUIRED]:
            Unique API Key to access Barchart's data.
    Returns:
        (None) Saves data in .csv file to provided destination path.
    """

    # start date is 30 days from today's date
    start_date = datetime.datetime.now() + datetime.timedelta(-delay)
    start_date = start_date.strftime('%Y%m%d')

    # initiate client for barchart API
    API_KEY = barchartKey
    client = ondemand.OnDemandClient(api_key=API_KEY, end_point="https://marketdata.websol.barchart.com/")

    # get daily stock data for company
    data = client.history(company, 'daily', startDate=start_date)['results']
    financial_headers = ['volume', 'timestamp', 'symbol', 'high', 'tradingDay', 'low', 'close', 'openInterest', 'open']
    f_data = csv.writer(open(destPath, "w"))

    # write csv data
    f_data.writerow(financial_headers)

    for data in data:
        f_data.writerow([data['volume'],
                         data['timestamp'],
                         data['symbol'],
                         data['high'],
                         data['tradingDay'],
                         data['low'],
                         data['close'],
                         data['openInterest'],
                         data['open']])



def get_twitter_data(consumer_key, consumer_secret, token_key, token_secret, handle, json_file, delay):

    """Get all tweets posted by {handle} in a given time frame determined with {delay}. The results are saved to {json_file}.
    If no tweet was found, tweets that mention {handle} will be saved instead in {json_file}
    Args:
        consumer_key (str) [REQUIRED]:
            Your unique consumer_key from twitter
        consumer_secret (str) [REQUIRED]:
            Your unique consumer_secret from twitter
        token_key (str) [REQUIRED]:
            Your unique token_key from twitter
        token_secret (str) [REQUIRED]:
            Your unique token_secret from twitter
        handle (str) [REQUIRED]:
            The twitter handle for which you wish to retrieve tweets for
        json_file (str) [OPTIONAL]:
            The json file path where you wish to save your tweet results. Default is "tweets.json"
        delay (int) [OPTIONAL]:
            Number of days from today's date you wish to get stock data for. Default is 30 days.
    Returns:
        (None) Saves tweets found in .json file to provided destination path. If no tweets found, nothing to be saved.
    """

    #authenticate to twitter
    api = twitter.Api(consumer_key=consumer_key,
                      consumer_secret=consumer_secret,
                      access_token_key=token_key,
                      access_token_secret=token_secret)



    # GetUserTimeLine method will return only up to 3200 most recent tweets.

    result = api.GetUserTimeline(screen_name=handle, count=3200)

    if (len(result) > 0):
        tweets =[tweet.AsDict() for tweet in result]
        tweets_posted = get_all_tweets(tweets, delay)

        if len(tweets_posted>0):
            # write json data
            with open(json_file, 'w') as outfile:
                json.dump(tweets_posted, outfile, indent=4, sort_keys=True)

    # GetSearch with the below query returns tweets that mention {handle}
    else:
        query = 'q=to%3A' + handle
        mentions = api.GetSearch(raw_query=query)

        if (len(mentions) > 0): #if there were any mentions found, write it to json file
            tweets = [tweet.AsDict() for tweet in mentions]
            tweets_mentionned = get_all_tweets(tweets, delay)

            if len(tweets_mentionned>0):
                #   write json data
                with open(json_file, 'w') as outfile:
                    json.dump(tweets_mentionned, outfile, indent=4, sort_keys=True)


def get_all_tweets(tweets, delay):


    """Parses found tweets and return only some fields in the tweets object, and only if the tweets were created in a timeframe determined by {delay}
    Args:
        tweets (Array) :
            Array of tweets returned from twitter api call.
        delay (int):
            Number of days from today's date you wish to get stock data for. Default is 30 days.
    Returns:
        (Array) Parsed tweet results.
    """

    now = datetime.datetime.now()
    start_date = now + datetime.timedelta(-delay)
    end_date = now

    tweets_found = []
    for tweet in tweets:
        if tweet["created_at"] < end_date and tweet["created_at"] > start_date:

            location = None
            retweet_count = 0
            if ("location" in tweet["user"]):
                location = tweet["user"]["location"]
            if ("retweet_count" in tweet):
                retweet_count = tweet["retweet_count"]
            res = {
                "created_at": tweet["created_at"],
                "text": tweet["text"],
                "location": location,
                "screen_name": tweet["user"]["screen_name"],
                "name": tweet["user"]["name"],
                "hashtags": tweet["hashtags"],
                "retweet_count": retweet_count
            }
            tweets_found.append(res)

    return tweets_found



### LOADING DATA TO BIGQUERY



def dataset_exists(client, dataset_reference):


    """Checks if a dataset exists on BigQuery
     Args:
           client (object) :
               Authenticated BigQuery Client
           dataset_reference (object):
                Dataset reference to check
       Returns:
           (Boolean) True if dataset already exists.
       """
    try:
        client.get_dataset(dataset_reference)
        return True
    except NotFound:
        return False


def load_csv_to_bq(csv_file, company):

    """Loads CSV data from local file system to BigQuery
     Args:
           csv_file (str) :
               Path to CSV file to upload to BigQuery
           company (str):
               Company symbol that will serve as dataset ID in BigQuery.
     Returns:
           (None) Prints upload job result to command line.
     """

    client = bq.Client()
    dataset_id = company
    dataset_ref = client.dataset(dataset_id)
    dataset = bq.Dataset(dataset_ref)
    if not dataset_exists(client, dataset_ref):
        dataset = client.create_dataset(dataset)

    table_id = 'stocks'
    table_ref = dataset_ref.table(table_id)
    job_config = bq.LoadJobConfig()
    job_config.source_format = bq.SourceFormat.CSV
    job_config.skip_leading_rows = 1
    job_config.autodetect = True


    with open(csv_file, 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location='US',
            job_config=job_config)  # API request

    job.result()  # Waits for table load to complete.

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))

def get_newline_json(json_file):

    """Converts JSON to NEWLINE DELIMITED JSON for BigQuery ingest
        Args:
              json_file (str) :
                  Path to json file to upload to BigQuery
          Returns:
              (str) Path to a new .json file. File is named oldfilename+'_nld.json'. This will be the file to upload to bigquery.
          """

    with open(json_file) as file:
        data = json.load(file)
    nld_data = ndjson.dumps(data)

    filename = os.path.splitext(os.path.basename(json_file))[0] + '_nld.json'
    with open(filename, 'w') as new_file:
        new_file.write(nld_data)
    return filename


def load_json_to_bq(json_file,company, handle):

    """Loads JSON data from local file system to BigQuery
       Args:
             json_file (str) :
                 Path to json file to upload to BigQuery
             company (str):
                 Company symbol that will serve as dataset ID in BigQuery.
             handle (str):
                 Twitter handle that will be a part of the table ID in BigQuery.

       Returns:
             (None) Prints upload job result to command line.
    """


    if not(os.path.exists(json_file)):
        print("No tweet was created by {} : nothing to upload to BigQuery".format(handle))
        return

    #convert JSON to newline delimited JSON format

    file_to_load = get_newline_json(json_file)

    client = bq.Client()
    dataset_id = company
    dataset_ref = client.dataset(dataset_id)
    dataset = bq.Dataset(dataset_ref)
    if not dataset_exists(client, dataset_ref):
        dataset = client.create_dataset(dataset)

    table_id = 'tweets' + '_' + handle
    table_ref = dataset_ref.table(table_id)
    job_config = bq.LoadJobConfig()
    job_config.autodetect = True
    job_config.source_format = bq.SourceFormat.NEWLINE_DELIMITED_JSON
    with open(file_to_load, 'rb') as source_file:
        job = client.load_table_from_file(
            source_file,
            table_ref,
            location='US',
            job_config=job_config)

    job.result()

    print('Loaded {} rows into {}:{}.'.format(
        job.output_rows, dataset_id, table_id))




def run():
    """Parses command line arguments and calls other methods.

       Returns:
             (None)
    """
    parser = argparse.ArgumentParser()
    # financial data from barchart API

    parser.add_argument("--symbol", dest="COMPANY", required=True, help="Enter the symbol of the company you want data for")
    parser.add_argument("--fromDays", dest="FROM_DAYS", help="Enter the number of days you want to go back to", default=30)
    parser.add_argument("--filePath", dest="FILEPATH", help="Enter the path to the csv file you wish to create from stock data.", default="financial_data.csv")
    parser.add_argument("--barchartKey", dest="BARCHART_KEY", required=True,help="Enter the unique key provided to you by barchart")

    #twitter args
    parser.add_argument("--consumerKey", dest="CONSUMER_KEY", required=True, help="Enter your twitter API consumer key")
    parser.add_argument("--consumerSecret", dest="CONSUMER_SECRET", required=True, help="Enter your twitter API consumer secret")
    parser.add_argument("--tokenKey", dest="TOKEN_KEY", required=True, help="Enter your twitter API access token key")
    parser.add_argument("--tokenSecret", dest="TOKEN_SECRET", required=True,help="Enter your twitter API access token secret")
    parser.add_argument("--twitterHandle", dest="TWITTER_HANDLE", required=True,help="Enter the twitter user name you wish to get data for")
    parser.add_argument("--jsonFilePath", dest="JSON_FILEPATH", help="Enter the full path where you would like to save your JSON results", default="tweets.json")

    app_args= parser.parse_args()
    get_financial_data(app_args.COMPANY,app_args.FROM_DAYS,app_args.FILEPATH, app_args.BARCHART_KEY)
    get_twitter_data(app_args.CONSUMER_KEY, app_args.CONSUMER_SECRET, app_args.TOKEN_KEY, app_args.TOKEN_SECRET, app_args.TWITTER_HANDLE, app_args.JSON_FILEPATH, app_args.FROM_DAYS)
    load_csv_to_bq(app_args.FILEPATH,app_args.COMPANY)
    load_json_to_bq(app_args.JSON_FILEPATH, app_args.COMPANY, app_args.TWITTER_HANDLE)



if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    run()