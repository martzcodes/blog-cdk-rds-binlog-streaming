#!/usr/bin/env python3
# -*- coding: utf-8 -*-
import json
import boto3
import os
import time
import datetime
import traceback

from pymysqlreplication import BinLogStreamReader
from pymysqlreplication.row_event import (
  DeleteRowsEvent,
  UpdateRowsEvent,
  WriteRowsEvent,
)
from decimal import Decimal

from boto3.dynamodb.types import TypeSerializer, TypeDeserializer

s3 = boto3.client('s3')
secretsmanager = boto3.client('secretsmanager')

def primary_keys(primaryKey, data):
  keys = []
  columns = ''
  values = ''
  types = ''
  if type(primaryKey).__name__ != 'tuple':
    keys = [primaryKey]
  else:
    if (len(primaryKey) == 1):
      for key in primaryKey[0]:
        keys.append(key)
    else:
      for key in primaryKey:
        keys.append(key)
  keys.sort()
  for idx, key in enumerate(keys):
    if (idx != 0):
      columns += '-'
      values += '-'
      types += '-'
    columns += key
    values += str(data[key])
    types += type(data[key]).__name__
  return {
    "columns": columns,
    "values": values,
    "types": types
  }

def handler(event, context):
  get_secret_value_response = secretsmanager.get_secret_value(SecretId=os.environ.get('SECRET_NAME'))
  secret_string = get_secret_value_response['SecretString']
  db = json.loads(secret_string)
  connectionSettings = {
    "host": db['host'],
    "port": 3306,
    "user": db['username'],
    "passwd": db['password']
  }

  ts = TypeSerializer()
  week = datetime.datetime.today() + datetime.timedelta(days=7)

  skipToTimestamp = None
  if os.environ.get('SKIP_TO_TIMESTAMP_ENABLED') == '1':
    # read meta file from s3
    get_meta = s3.get_object(Bucket=os.environ.get("BUCKET_NAME"),Key="meta.json")
    meta_json = json.loads(get_meta['Body'].read().decode('utf-8'))
    skipToTimestamp=int(meta_json['timestamp']) # make sure it's an int and not a decimal
  print("skipToTimestamp: {} {}".format(skipToTimestamp, type(skipToTimestamp).__name__))

  stream = BinLogStreamReader(
    connection_settings=connectionSettings,
    server_id=int(os.environ.get('SERVER_ID')),
    resume_stream=False,
    only_events=[DeleteRowsEvent, WriteRowsEvent, UpdateRowsEvent], # inserts, updates and deletes
    only_tables=None, # a list with tables to watch
    skip_to_timestamp=skipToTimestamp,
    ignored_tables=None, # a list with tables to NOT watch
    )

  totalEventCount = 0
  errorCount = 0
  dataToStore = {}
  dataToStoreCount = {}
  dataToStoreLastTimestamp = {}
  for binlogevent in stream:
    if binlogevent.table not in dataToStore:
      dataToStore[binlogevent.table] = []
      dataToStoreCount[binlogevent.table] = 0
      dataToStoreLastTimestamp[binlogevent.table] = 0
    for row in binlogevent.rows:
      totalEventCount += 1
      if totalEventCount % 1000 == 0:
        print("Processed {} with {} errors".format(totalEventCount, errorCount))
      row_keys = { "columns": "unknown", "values": "unknown", "types": "unknown"}
      normalized_row = json.loads(json.dumps(row, indent=None, sort_keys=True, default=str, ensure_ascii=False), parse_float=Decimal)
      delta = {}
      if type(binlogevent).__name__ == "UpdateRowsEvent":
        row_keys = primary_keys(binlogevent.primary_key, normalized_row['after_values'])
        after = {}
        before = {}
        for key in normalized_row["after_values"].keys():
          if normalized_row["after_values"][key] != normalized_row["before_values"][key]:
            after[key] = normalized_row["after_values"][key]
            before[key] = normalized_row["before_values"][key]
        delta["after"] = json.dumps(after, indent=None, sort_keys=True, default=str, ensure_ascii=False)
        delta["before"] = json.dumps(before, indent=None, sort_keys=True, default=str, ensure_ascii=False)
      else:
        row_keys = primary_keys(binlogevent.primary_key, normalized_row['values'])

      event = {
        "PK": "{0}#{1}".format(binlogevent.schema, binlogevent.table),
        "SK": "{0}#{1}#{2}".format(row_keys['values'], binlogevent.timestamp, type(binlogevent).__name__),
        "primaryIdColumns": row_keys['columns'],
        "primaryIdValues": row_keys['values'],
        "primaryIdTypes": row_keys['types'],
        "schema": binlogevent.schema,
        "table": binlogevent.table,
        "type": type(binlogevent).__name__,
        "ttl": int(time.mktime(week.timetuple())),
      }

      if type(binlogevent).__name__ == "UpdateRowsEvent":
        event["delta"] = delta

      dataToStore[binlogevent.table].append(event)
      dataToStoreCount[binlogevent.table] += 1
      dataToStoreLastTimestamp[binlogevent.table] = binlogevent.timestamp
      dataToStore["timestamp"] = binlogevent.timestamp
  print("COMPLETED {} events with {} errors".format(totalEventCount, errorCount))
  print(json.dumps(dataToStoreCount, indent=None, sort_keys=True, default=str))

  s3.put_object(
    Body=json.dumps({ "lastTimestamps": dataToStoreLastTimestamp, "counts": dataToStoreCount, "tables": dataToStore}, indent=None, sort_keys=True, default=str),
    Bucket=os.environ.get("BUCKET_NAME"),
    Key="binlog-{}.json".format(str(meta_key["timestamp"])))
  print("Done with combined file")
  
  for s3Table in dataToStore.keys():
    s3.put_object(
    Body=json.dumps({ "lastTimestamp": dataToStoreLastTimestamp, "count": dataToStoreCount[s3Table], "events": dataToStore[s3Table]}, indent=None, sort_keys=True, default=str),
    Bucket=os.environ.get("BUCKET_NAME"),
    Key="{}/binlog-{}.json".format(s3Table, str(meta_key["timestamp"])))
  print("done with individual tables")
  
  s3.put_object(
    Body=json.dumps({ "lastTimestamps": dataToStoreLastTimestamp, "counts": dataToStoreCount, "tables": dataToStore}, indent=None, sort_keys=True, default=str),
    Bucket=os.environ.get("BUCKET_NAME"),
    Key="meta.json")
  print("Done with meta file")

if __name__ == "__main__":
   handler()