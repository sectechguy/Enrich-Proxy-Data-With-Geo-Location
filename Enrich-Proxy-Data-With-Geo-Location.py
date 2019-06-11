import geoip2.database
from geolite2 import geolite2
from impala.dbapi import connect
from impala.util import as_pandas
import pandas as pd
import numpy as np

cursor.execute('select time,statuscode, name, request, useragentstring, reqmeth, srchstname, srcaddr, date \
from rev_tab \
where time BETWEEN concat(to_date(now() - interval 1 days), "T00:00:00.000Z") and concat(to_date(now() - interval 1 days), "T23:59:59.999Z")')
rev_prox = as_pandas(cursor)

#REMOVE IPS == NONE
rev_prox['srcaddr'] = rev_prox['srcaddr'].replace('None', np.nan)
rev_prox = rev_prox.dropna(axis=0, subset=['srcaddr']).reset_index(drop=True)

#FUNCTION TO GRAB LATITUDE
def get_latitude(ip):
    try:
       rev_prox = geo.get(ip)
    except ValueError:
        return pd.np.nan
    try:
        return rev_prox['location']['latitude'] if rev_prox else pd.np.nan
    except KeyError:
        return pd.np.nan
geo = geolite2.reader()
#GET UNIQUE IPS
unique_ips = rev_prox['srcaddr'].unique()
#MAKE A SERIES
unique_ips = pd.Series(unique_ips, index = unique_ips)
#MAP IP TO LATITUDE
rev_prox['latitude'] = rev_prox['srcaddr'].map(unique_ips.apply(get_latitude))
#CLOSE CONNECTION
geolite2.close()

#FUNCTION TO GRAB LONGITUDE
def get_longitude(ip):
    try:
       rev_prox = geo.get(ip)
    except ValueError:
        return pd.np.nan
    try:
        return rev_prox['location']['longitude'] if rev_prox else pd.np.nan
    except KeyError:
        return pd.np.nan
geo = geolite2.reader()
#GET UNIQUE IPS
unique_ips = rev_prox['srcaddr'].unique()
#MAKE A SERIES
unique_ips = pd.Series(unique_ips, index = unique_ips)
#MAP IP TO LONGITUDE
rev_prox['longitude'] = rev_prox['srcaddr'].map(unique_ips.apply(get_longitude))
#CLOSE CONNECTION
geolite2.close()

#FUNCTION TO GRAB COUNTRY
def get_country(ip):
    try:
       rev_prox = geo.get(ip)
    except ValueError:
        return pd.np.nan
    try:
        return rev_prox['country']['names']['en'] if rev_prox else pd.np.nan
    except KeyError:
        return pd.np.nan
geo = geolite2.reader()

#GET UNIQUE IPS
unique_ips = rev_prox['srcaddr'].unique()
#MAKE A SERIES
unique_ips = pd.Series(unique_ips, index = unique_ips)
#MAP IP TO COUNTRY
rev_prox['country'] = rev_prox['srcaddr'].map(unique_ips.apply(get_country))
#CLOSE CONNECTION
geolite2.close()

#PYSPARK
from pyspark.sql import SQLContext
from pyspark.sql import *
from pyspark.sql.types import *
from pyspark import SparkContext
spark = SparkSession.builder.appName('revproxy').getOrCreate()

schema = StructType([StructField('Time', StringType(), False),
                     StructField('StatusCode', StringType(), True),
                     StructField('StatusName', StringType(), True),
                     StructField('Request', StringType(), True),
                     StructField('UserAgentString', StringType(), True),
                     StructField('RequestMethod', StringType(), True),
                     StructField('SourceHostName', StringType(), True),
                     StructField('SourceAddress', StringType(), True),
                     StructField('Date', StringType(), True),
                     StructField('Latitude', StringType(), True),
                     StructField('Longitude', StringType(), True),
                     StructField('Country', StringType(), True)])

dataframe = spark.createDataFrame(rev_prox, schema)
dataframe.write. \
  mode("append"). \
  option("path", "/user/hive/warehouse/<db_name>.db/enriched_reverse_proxy"). \
  saveAsTable("<db_name>.enriched_reverse_proxy")
