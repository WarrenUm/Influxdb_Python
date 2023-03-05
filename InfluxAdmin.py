from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS
import pandas as pd
import numpy as np
import requests
import json
from datetime import datetime
import sqlite3
from sqlite3 import Error
import csv
import os
from tqdm.notebook import tqdm_notebook
from tqdm import tqdm
import time
import shutil
from time import perf_counter_ns
from dateutil import parser

dbURL = "http://localhost:8086"
userToken = "Cxm9NsNCekh3eC1OWCQQ3ENhPUnU4u1ODq449VplcwRUoS7IDzTHtTKX_3Bl1DnvVPxQnEhTeCLAR3ab5msmEg=="
dbOrg = "Ge-data-project"
dbBucket = "GEItemPrices"

def GetDatabaseClient():
    return InfluxDBClient(url=dbURL,token=userToken,org=dbOrg)

def WriteToDatabase(client, records):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=dbBucket,write_precision='s',record=records)

def CreateFluxQuery():
    fluxString = '''
    from(bucket: _bucket)
    |> range(start: _timeStart)
    |> filter(fn: (r) => r.itemID == _itemID)
    |> pivot(rowKey:["_time"], columnKey: ["_field"], valueColumn: "_value")
    |> keep(columns: ["avgHighPrice", "avgLowPrice","highPriceVolume","lowPriceVolume","_time","itemID"])
    '''
    return fluxString

def GetDatabaseQuery(client,fluxString,params):
    query_api = client.query_api()
    data_frame = query_api.query_data_frame(query=fluxString,params=params)
    return data_frame

def QueryDatabase(client,startTime,itemID,bucket=dbBucket):
    params = {
        "_bucket": bucket,
        "_timeStart": startTime,
        "_itemID": itemID
    }
    fluxString = CreateFluxQuery()
    return GetDatabaseQuery(client,fluxString,params)

def get5mItemData(timestamp):
    headers = {'User-Agent':'GEoutlier-detection'}
    api_endpt = "https://prices.runescape.wiki/api/v1/osrs"
    five_min_timestamp = "/5m?timestamp="
    items_response = requests.get((api_endpt+five_min_timestamp+str(timestamp)), headers=headers)
    return items_response

def getDFAsJson(timestamp):
    response = get5mItemData(timestamp)
    jsonData = json.loads(response.text)
    return jsonData

def incrementTime(oldTime):
#   print('Incrementing Time')
    return oldTime + 300

def updateTimeFile(newTime,file):
    with open(file,'w') as file:
        #overwrite first line with used timestamp
#     print(f'Updating Time File With: {newTime}')
        file.writelines(newTime)
    return newTime

def getLatestTimestamp(file):
    with open(file,'r') as file:
        #read line
#     print("Reading New time")
        timeLine = file.readline()
    return int(timeLine)


