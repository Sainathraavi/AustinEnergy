import json
import boto3
from boto3.dynamodb.conditions import Key, Attr

import traceback as tb
import os
import datetime
import time
import decimal
import concurrent.futures
from dateutil.parser import parse
import tenacity

import logging
import sys

#logging.basicConfig(stream=sys.stdout)#, level=logging.INFO)
FORMAT = "%(asctime)s %(levelname)s %(process)s %(thread)s: %(message)s"

#logging.basicConfig(format=FORMAT, filemode='w', filename='dynamodb-etl.log',level=logging.DEBUG)
logging.basicConfig(format=FORMAT, filename='dynamodb-etl.log',level=logging.INFO)

logger = logging.getLogger(__name__)



class MeterQuery:

    def __init__ (self, meter_table, table_from, table_to, start_time, end_time, num_threads=3):
        self.meter_table_name = meter_table
        self.table_from_name = table_from
        self.table_to_name = table_to
        self.start_time = start_time
        self.end_time = end_time
        self.num_threads = num_threads

        try:
            self.dynamodb = boto3.resource('dynamodb')
            self.meter_table = self.dynamodb.Table(self.meter_table_name)

        except Exception, e:
            raise e

    def queryMeters(self):
        result = self.meter_table.scan()
        #print result['Items']

        end_time = parse(self.end_time)
        start_time = parse(self.start_time)
        
        logger.info('startTime=%s, endTime=%s' % (str(start_time), str(end_time), ))
        time_start = decimal.Decimal(time.mktime(start_time.timetuple()))
        time_end = decimal.Decimal(time.mktime(end_time.timetuple()))
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.num_threads) as executor:
            todo = []
            for meter in result['Items']:
                if meter['Enabled'] == True:
                    #self.queryMeter(meter['MeterMacId'], meter['Meter'], meter['CloudID'], time_start, time_end)
                    future = executor.submit(self.queryMeter, meter['MeterMacId'], meter['Meter'], meter['CloudID'], time_start, time_end)
                    todo.append(future)
            results = []
            for future in concurrent.futures.as_completed(todo):
                res = future.result()
                results.append(res)
                #print(len(results))

    @tenacity.retry(wait=tenacity.wait_exponential() + tenacity.wait_random(0, 2),
        before=tenacity.before_log(logger, logging.DEBUG),
        after=tenacity.after_log(logger, logging.DEBUG))
    def queryMeter(self, meterMacId, meterID, cloudID,  time_start, time_end):
        try:

            dynamodb = boto3.resource('dynamodb')
            
            table_to = self.dynamodb.Table(self.table_to_name)

            expression=Key('MeterMacId').eq(meterMacId) & Key('TimeStamp').between(time_start, time_end)
            result = table_to.query(KeyConditionExpression=expression)
            data = result['Items']
            
            if (len(data) > 0):
                while 'LastEvaluatedKey' in result:
                    # time.sleep(1)
                    result = table_to.query(KeyConditionExpression=expression, 
                            ExclusiveStartKey=result['LastEvaluatedKey'])
                    data.extend(result['Items'])
                logger.info ('meterId= %s, cloudId=%s, meterMacId=%s, timeStart=%s, timeEnd=%s, numItems=%d, slices=%d, remainder=%d' %
                (meterID, cloudID, meterMacId, str(time_start), str(time_end), len(data), len(data)/25, len(data)%25))
                self.writeData(data)



        except Exception, e:
            raise e



    def writeData(self, data):
        try:
            # loop through the array in steps of 25
            start=0
            i=0
            mod = len(data) % 25
            request_items = None
            for i in range(0, int(len(data)/25)):
                
                start=i*25
                sub_data = data[start:start+25]
                request_items = {}
                request_items[self.table_to_name] = self.populatePutRequests(sub_data)    
                #result = self.target_dynamodb.batch_write_item(RequestItems=request_items,
                #    ReturnConsumedCapacity='TOTAL',
                #    ReturnItemCollectionMetrics='SIZE') 
                #print result
                self.batch_write(request_items)
                #print len(request_items[self.demand_table_name])           
                #print request_items
                #print i

            if (mod != 0):
                if (request_items is not None):
                    start = (i+1) * 25
                else:
                    start = 0
                sub_data=data[start:start+mod]
                request_items = {}
                request_items[self.table_to_name] = self.populatePutRequests(sub_data)
                self.batch_write(request_items)
                #print result                
                #print len(request_items[self.demand_table_name] )            
                #print request_items
                
                #print sub_data

        except Exception, e:
            logger.error('Traceback: %s' % (tb.format_exc(),))
            raise e

    def populatePutRequests(self, data):
        put_array = []
        for sub in data:
            item = {}
            item['PutRequest'] = {}
            item['PutRequest']['Item'] = sub
            put_array.append(item)

        return put_array

    @tenacity.retry(wait=tenacity.wait_exponential() + tenacity.wait_random(0, 2),
        retry=(tenacity.retry_if_result(lambda result: result['UnprocessedItems'] != {})  |
           tenacity.retry_if_result(lambda result: result['ResponseMetadata']['HTTPStatusCode'] != 200 ) | 
           tenacity.retry_if_exception_type(Exception)), 
        before=tenacity.before_log(logger, logging.DEBUG),
        after=tenacity.after_log(logger, logging.DEBUG))

    def batch_write(self, request_items):
        result = self.access_remote_dynamodb().batch_write_item(RequestItems=request_items,
            ReturnConsumedCapacity='TOTAL',
            ReturnItemCollectionMetrics='SIZE')
        return result

    def access_remote_dynamodb(self):
        client = boto3.client('sts')
        sts_response = client.assume_role(RoleArn=os.environ['TARGET_ROLE_ARN'],                              
                        RoleSessionName='AssumeRoleLoadCoopDynamoDB', 
                        DurationSeconds=int(os.environ['TARGET_ROLE_DURATION']))
                        
        accessKey = sts_response['Credentials']['AccessKeyId']
        secret = sts_response['Credentials']['SecretAccessKey']
        sessionToken = sts_response['Credentials']['SessionToken']
        target_dynamodb = boto3.resource(service_name='dynamodb', 
            region_name=os.environ['TARGET_AWS_REGION'],
            aws_access_key_id = accessKey,
            aws_secret_access_key = secret,
            aws_session_token = sessionToken)
        
        #target_demand_table = target_dynamodb.Table(self.demand_table_name)
        logger.info("Accessed remote DynamoDb resource.")        
        return target_dynamodb
                

def lambda_handler(event, context):
    try:
        start_date = os.environ['START_TIME']
        end_date = os.environ['END_TIME']
        if 'NUM_THREADS' not in os.environ: 
            num_threads = 3 
        else: 
            num_threads=int(os.environ['NUM_THREADS'])

        if 'TABLE_FROM' not in os.environ:
            table_from = 'rainforest-instantaneous-demand'
        else:
            table_from = os.environ['TABLE_FROM']

        if 'TABLE_TO' not in os.environ:
            table_to = table_from
        else:
            table_to = os.environ['TABLE_TO']

        meter_query = MeterQuery( 'meters', table_from, table_to, start_date, end_date, num_threads)
        meter_query.queryMeters()
        
        return 'Successfully Completed'
        
    except Exception, e:
        logger.error( str(e) + '\n' + tb.format_exc())
        return str(e)


if __name__ == '__main__':
    logger.info( lambda_handler(None, None))