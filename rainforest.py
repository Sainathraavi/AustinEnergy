import boto3
from boto3.dynamodb.conditions import Key, Attr
import json
import encoder
import datetime
import traceback
from decimal import Decimal
from datetime import datetime, timedelta
import time
import collections

MeterStatus = collections.namedtuple('Status', ['enabled', 'logDemand'])


def write_error(dynamodb, msg, s3):
    item = {}
    try:
        item['TimeStamp'] = Decimal(str(int(time.time())))
        item['Payload'] = msg
        table = dynamodb.Table('rainforest-errors')
        table.put_item(Item=item)
    except Exception, e:
        tb = traceback.format_exc()
        s3.Object('rainforest-eagle', 'post-data-error.txt').put(Body=bytearray(tb))
        s3.Object('rainforest-eagle', 'post-data.txt').put(Body=bytearray(msg))

def process_catch_basin(msg, dynamodb, s3):
    item = {}
    try:
        msg['TimeStamp'] = Decimal(str(int(time.time())))
#        item['Message'] = msg
        table = dynamodb.Table('rainforest-catch-basin')
        table.put_item(Item=msg)
    except Exception, e:
        tb = traceback.format_exc()
        write_error(dynamodb, tb + '\n' + msg, s3)


def unix_time_millenium_epoch(timestamp):
    base = datetime(2000, 1, 1) # start of your epoch
    return base + timedelta(seconds=timestamp)


def get_unix_time(timestamp):
    epoch = datetime.utcfromtimestamp(0)
    modified_ts = unix_time_millenium_epoch(timestamp)
    return (modified_ts - epoch).total_seconds()

def get_time_from_epoch(timestamp):
    t = datetime.utcfromtimestamp(timestamp)
    return t.strftime("%B %d, %Y %H:%M:%S UTC")

#def process_rainforest_attributes(rainforest):
#    item = {}
#    return item

def process_base_attributes(rainforest):
    item = {}
    ts = get_unix_time(int(rainforest['TimeStamp'][2:], 16))
    item['TimeStamp'] = Decimal(str(ts))
    item['TimeStampStr'] = get_time_from_epoch(ts)
    item['DeviceMacId'] = rainforest['DeviceMacId'][2:]
    item['MeterMacId'] = rainforest['MeterMacId'][2:]
    item['Divisor'] = int(rainforest['Divisor'][2:], 16)
    if item['Divisor'] == 0.0: rainforest['Divisor'] = 1.0

    item['Multiplier'] = int(rainforest['Multiplier'][2:], 16)
    if item['Multiplier'] == 0.0: item['Multiplier'] = 1.0
    return item

def process_network_info(timestamp, cloudId, network, dynamodb, s3):

    try:
        item = {}
        item['CloudId'] = cloudId
        item['MeterMacId'] = network['CoordMacId'][2:]
        item['DeviceMacId'] = network['DeviceMacId'][2:]
        item['TimeStamp'] = Decimal(timestamp)
        item['TimeStampStr'] = get_time_from_epoch(int(timestamp))
        item['Channel'] = network['Channel']
        if (len(network['Description']) > 0):
            item['Description'] = network['Description']
        item['Status'] = network['Status']
        link = int(network['LinkStrength'][2:], 16)
        item['LinkStrength'] = Decimal(str(link))
        table = dynamodb.Table('rainforest-network-info')
        table.put_item(Item=item)
    except Exception, e:
        tb = traceback.format_exc()
        write_error(dynamodb, tb + '\n' + json.dumps(network), s3)


def process_current_summation(ts, cloudId, summation, dynamodb, s3):
    item = process_base_attributes(summation)
    try:
        item['CloudId'] = cloudId
        item['SummationDelivered'] = int(summation['SummationDelivered'][2:], 16)
        summation_calc = float(item['SummationDelivered']) * item['Multiplier'] / item['Divisor']
        item['kWHDelivered'] = Decimal(str(summation_calc))

        item['SummationReceived'] = int(summation['SummationReceived'][2:], 16)
        summation_calc = float(item['SummationReceived']) * item['Multiplier'] / item['Divisor']
        item['kWHReceived'] = Decimal(str(summation_calc))

        table = dynamodb.Table('rainforest-current-summation')
        table.put_item(Item=item)

    except Exception, e:
        tb = traceback.format_exc()
        write_error(dynamodb, tb + '\n' + json.dumps(summation), s3)

def process_instantaneous_demand(ts, cloudId, demand, dynamodb, s3):
    item = process_base_attributes(demand)
    try:
        item['CloudId'] = cloudId
        if (demand['Demand'][2:6] == 'ffff'): 
            multiplier = -1
            item['Demand'] = int(demand['Demand'][6:], 16) - 65536
        else:
            item['Demand'] = int(demand['Demand'][2:], 16)
        demand_calc = float(item['Demand']) * item['Multiplier'] / item['Divisor']
        item['KWDemand'] = Decimal(str(demand_calc))
        table = dynamodb.Table('rainforest-instantaneous-demand')
        table.put_item(Item=item)

    except Exception, e:
        tb = traceback.format_exc()
        write_error(dynamodb, tb + '\n' + json.dumps(demand), s3)
        
def checkMeterValid(meterMacId, meterTable):
    expression=Key('MeterMacId').eq(meterMacId)
    result = meterTable.query(KeyConditionExpression=expression)
    enabled = False
    logDemand = False
    
    if (len(result['Items']) == 1):
        enabled =  result['Items'][0]['Enabled']
        logDemand = result['Items'][0]['LogDemand']

    return MeterStatus(enabled, logDemand)
    
def lambda_handler(event, context):

    try:
        s3 = boto3.resource('s3')
        dynamodb = boto3.resource('dynamodb')
        meterTable = dynamodb.Table('meters')
        obj = encoder.XML2Dict(coding='utf-8')

        dt = obj.parse(event['body'])
        rf = dt['rainforest']
        rfa = dt['@rainforest']

        if 'InstantaneousDemand' in rf:
            meterStatus = checkMeterValid(rf['InstantaneousDemand']['MeterMacId'][2:], meterTable)
            if (meterStatus.enabled):
                if (meterStatus.logDemand):
                    process_catch_basin(dt, dynamodb, s3)
                process_instantaneous_demand(rfa['timestamp'][:-1], rfa['macId'][2:], rf['InstantaneousDemand'], dynamodb, s3)
        elif 'CurrentSummationDelivered' in rf:
            if (checkMeterValid(rf['CurrentSummationDelivered']['MeterMacId'][2:], meterTable).enabled):
                process_current_summation(rfa['timestamp'][:-1], rfa['macId'][2:], rf['CurrentSummationDelivered'], dynamodb, s3)
        elif 'NetworkInfo' in rf:
           if (checkMeterValid(rf['NetworkInfo']['CoordMacId'][2:], meterTable).enabled):
               process_network_info(rfa['timestamp'][:-1], rfa['macId'][2:], rf['NetworkInfo'], dynamodb, s3)
        elif 'DeviceInfo' in rf:
            pass
        elif 'MessageCluster' in rf:
            pass
        elif 'PriceCluster' in rf:
            pass
        else:
            process_catch_basin(dt, dynamodb, s3)

    except Exception as e:
        tb = traceback.format_exc()
        write_error(dynamodb, tb + '\n' + json.dumps(dt), s3)

    return '\n'
