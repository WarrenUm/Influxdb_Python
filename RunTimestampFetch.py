from distutils.log import error
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
from InfluxAdmin import *
import sys



def main():
    now = round(datetime.timestamp(datetime.now()))
    newTime = getLatestTimestamp('latestTime.txt')
    timeRange = np.arange(newTime,now,300)
    for i in tqdm(timeRange):
        timestamp = getLatestTimestamp("latestTime.txt")
        timeJsonData = getDFAsJson(timestamp)
        timeJsonDataTimestamp = timeJsonData['timestamp']
        timeJsonDataData = timeJsonData['data']
        records = []
        for itemID in timeJsonDataData.keys():
            measure = timeJsonDataData[itemID]
            record = {
                "measurement": "itemData",
                "tags": {"itemID": str(itemID)},
                "fields": {"avgHighPrice": measure['avgHighPrice'],
                        "avgLowPrice": measure['avgLowPrice'],
                        "highPriceVolume": measure['highPriceVolume'],
                        "lowPriceVolume": measure['lowPriceVolume']},
                "time": timeJsonDataTimestamp
            }
            records.append(record)
            
        #sendto InfluxDB
        client = GetDatabaseClient()
        WriteToDatabase(client, records)
        client.close()
        oldTime = timestamp
        newTime = incrementTime(oldTime)
        updateTimeFile(str(newTime),"latestTime.txt")
        time.sleep(np.random.randint(0.2,2))


main()