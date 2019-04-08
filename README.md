## ETL BARCHART API & TWITTER API TO BIGQUERY

### **Goal:**
The purpose of this project is to:
1. Get Apple's stock data for the past 30 days using Barchart OnDemand's free market data APIs.
2. Get Apple's tweets for the past 30 days (if possible).
3. Load data into BigQuery.


### **Deliverables:**
In this repository, you will find three files of interest:
1. _Apple_data.py:_ contains the code to get the stock data from Barchart API, the tweets data from the Twitter API, and to load both data into BigQuery.
2. _Data Audit & Analysis - Stock Data.ipynb:_ contains data audit, analysis from Apple's stock market data. 
3. _Data Audit & Analysis - Tweets.ipynb:_ contains data audit, analysis, and insights from @realDonaldTrump's tweets. 
4. BigQuery dataset can be publicly viewed here: https://console.cloud.google.com/bigquery?project=test-project-datalab-225214&p=test-project-datalab-225214&d=AAPL&t=tweets_realdonaldtrump&page=table

### **Requirements:**
1. Clone the repository and install necessary packages.
2. You will also need to have a Google Cloud Platform account & project. 
3. You will need a service account with at least Editor role in BigQuery. 
4. You will need Barchart API_KEY, Twitter: {CONSUMER_KEY, CONSUMER_SECRET, TOKEN_KEY, TOKEN_SECRET}

### **Running the code:**
In your terminal:

`python Apple_data.py --symbol='COMPANY_SYMBOL' --consumerKey='YOUR_CONSUMER_KEY' --consumerSecret='YOUR_CONSUMER_SECRET' --tokenKey='YOUR_TOKEN_KEY' --tokenSecret='YOUR_TOKEN_SECRET' --twitterHandle="YOUR_TWITTER_HANDLE_CHOICE" --barchartKey='YOUR_BARCHART_API_KEY'`

To make the job a recurring one, add the python script to crontab:

``crontab -e``

Edit crontab similar to: 

`*/10 * * * *   /usr/bin/python /path/to/you/python/script.py`

Restart cron for job to occur:

`sudo systemctl restart cron`

### **Notes:**
1. @Apple has not posted any tweets, nor has @Apple been mentioned in other user tweets. Thus, for the purpose of demonstrating a functional ETL code, I have used @realDonaldTrump instead.
2. This BigQuery dataset will be deleted after 2 weeks.

### **To improve:**
1. Ensure duplicates are not stored in BigQuery before data ingestion.
2. Improve most frequent word count in tweets by trouble shooting stop words used. 
3. For portability, create a python package for code, along with the setup and requirement files.
4. For recurring job running on the Cloud, need to use App Engine Cron Service.
