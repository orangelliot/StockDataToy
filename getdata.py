import os
os.environ['PYARROW_IGNORE_TIMEZONE']='1'
os.environ['PYSPARK_DRIVER_PYTHON']='C:/Users/ellio/miniconda3/envs/bigdata/python.exe'
os.environ['PYSPARK_PYTHON']='C:/Users/ellio/miniconda3/envs/bigdata/python.exe'

import requests
import pyspark
import pyspark.pandas as ps
from pyspark.sql import SparkSession
from dotenv import load_dotenv

with SparkSession\
    .builder\
    .master('local')\
    .appName("stockdatatoy")\
    .getOrCreate() as spark:

    nasdaq_screener = ps.read_csv('nasdaq_screener.csv', index_col="Symbol")

    print(nasdaq_screener.info())

load_dotenv()

AV_KEY = os.getenv('AV_KEY')
FUNCTION = 'TIME_SERIES_INTRADAY'
SYMBOL = 'IBM'
INTERVAL = '5min'

exit()

url = f'https://www.alphavantage.co/query?function=SYMBOL_SEARCH&keywords=type&apikey={AV_KEY}'
r = requests.get(url)
print(r.json())

def get_stock_data(function, symbol, interval, apikey):
    url = f'https://www.alphavantage.co/query?function={function}&symbol={symbol}&interval={interval}&apikey={apikey}'
    r = requests.get(url)
    return r.json()