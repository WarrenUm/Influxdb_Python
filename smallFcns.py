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

# from osrs_fcns import *

def get5mItemData(timestamp):
    headers = {'User-Agent':'GEoutlier-detection'}
    api_endpt = "https://prices.runescape.wiki/api/v1/osrs"
    five_min_timestamp = "/5m?timestamp="
    items_response = requests.get((api_endpt+five_min_timestamp+str(timestamp)), headers=headers)
    return items_response

def getDFAsJson(timestamp):
    response = get5mItemData(timestamp)
    jsonData = json.loads(response.text)
    df = pd.DataFrame(data=[[timestamp,json.dumps(jsonData)]],columns=['timestamp','data'])
    return df

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
