import os
os.environ['PYARROW_IGNORE_TIMEZONE']='1'
os.environ['PYSPARK_DRIVER_PYTHON']='C:/Users/ellio/miniconda3/envs/bigdata/python.exe'
os.environ['PYSPARK_PYTHON']='C:/Users/ellio/miniconda3/envs/bigdata/python.exe'

import requests
import pyspark.pandas as ps
import pandas as pd
from pyspark.sql import SparkSession
from dotenv import load_dotenv
from alpha_vantage.timeseries import TimeSeries

load_dotenv()

AV_KEY = os.getenv('AV_KEY')
FUNCTION = 'TIME_SERIES_INTRADAY'
SYMBOL = 'IBM'
INTERVAL = '5min'


with SparkSession\
    .builder\
    .master('local')\
    .appName('stockdatatoy')\
    .getOrCreate() as spark:

    ts = TimeSeries(key=AV_KEY, output_format='pandas')

    nasdaq_screener = pd.read_csv('nasdaq_screener.csv', index_col='Symbol')

    industries = nasdaq_screener.Industry.unique()

    print(ts.get_weekly_adjusted('MSFT'))
    exit()
    for ind in industries:
        ind_companies = nasdaq_screener[nasdaq_screener['Industry'] == ind]
        weekly_ind_avg = None
        for company in ind_companies:
            ts.get_weekly_adjusted(company)