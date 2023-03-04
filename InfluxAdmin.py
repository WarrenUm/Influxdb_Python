from influxdb_client import InfluxDBClient
from influxdb_client.client.write_api import SYNCHRONOUS

dbURL = "http://localhost:8086"
userToken = "Cxm9NsNCekh3eC1OWCQQ3ENhPUnU4u1ODq449VplcwRUoS7IDzTHtTKX_3Bl1DnvVPxQnEhTeCLAR3ab5msmEg=="
dbOrg = "Ge-data-project"
dbBucket = "GEItemPrices"

def GetDatabaseClient():
    return InfluxDBClient(url=dbURL,token=userToken,org=dbOrg)

def WriteToDatabase(client, records):
    write_api = client.write_api(write_options=SYNCHRONOUS)
    write_api.write(bucket=dbBucket, record=records)

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



