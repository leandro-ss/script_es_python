
import os
import sys
import re
import time
import string
import psycopg2
import psycopg2.extras

from pytz     import utc, timezone
from datetime import date, datetime, timedelta
from elasticsearch import Elasticsearch, helpers

ES = Elasticsearch([{"host": "192.168.56.110"}])

BASE = "host='192.168.56.190' dbname='capacity'  user='elk' password='mokona69'"

def gen_ctl_file():
    return 'integracao/.ctl_%s' %(sys.argv[0].split('.')[0])

def get_index_id(*args):
    """ concat strings / remove special caracters
    """
    result = ''.join ( str(arg) for arg in args)
    result = re.sub('[%s]| ' % re.escape(string.punctuation), '', result)

    return result

def create_index(data, index_name, id_idx=None):
    """ create object compatible with index structure of ES
    """
    init_idx = {}
    init_idx['_type'] = 'py_custom'
    init_idx['_index'] = index_name
    init_idx['_source'] = data
    init_idx['_source']['@timestamp_import'] = datetime.now(utc)

    if id_idx:
        init_idx['_id'] = id_idx

    return init_idx

def get_last_process_date():
    """
    """
    try:
        with open(gen_ctl_file(), 'r') as f:
            dt = datetime.strptime(f.readline(), "%Y-%m-%dT%H:%M:%S %z")
    except Exception:
        dt = time_in_minute(datetime.now(utc), -1)

    return dt

def set_last_process_date():
    """
    """
    with open(gen_ctl_file(), 'w') as f:
        f.writelines(time_in_minute(datetime.now(utc), 0).strftime("%Y-%m-%dT%H:%M:%S %z"))

def time_in_minute(dt, minus_time):
    if minus_time < 0:
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, tzinfo=dt.tzinfo) - timedelta( minutes= - minus_time)
    else:
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, tzinfo=dt.tzinfo) + timedelta( minutes= + minus_time)

def datetime_utc2local(utc_datetime):
    now_timestamp = time.time()
    offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
    return utc_datetime + offset

def insert_ES(data, ES=ES):
    """
    """
    if data:
        helpers.bulk(ES, data)
        ES.indices.refresh(index=data[0]["_index"])
    set_last_process_date()

def get_cursor_postgres(str_conn=BASE):
    """ connect to the PostgreSQL database, create a new cursor
    """
    conn = psycopg2.connect(str_conn)
    return  conn.cursor()
