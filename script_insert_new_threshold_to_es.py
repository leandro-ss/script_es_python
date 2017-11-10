##################################################################################################################
## # autor: Leandro Sampaio Silva
## # V 0.0.0
################################################################################################################
import re
import string
import psycopg2
import psycopg2.extras
from pytz     import utc, timezone
from datetime import date, datetime, timedelta
from elasticsearch import Elasticsearch, helpers

#######################################################################################################################

ES = Elasticsearch([{"host": "192.168.56.110"}])

BASE = "host=192.168.56.190 database=capacity  user=elk password=mokona69"

IDX = "python-threshold" + date.today().strftime("-%Y-%m-%d")

CTL_FILE = ".ctl_exec_threshold"

#######################################################################################################################

def insert_ES(data, index_name=IDX):
  """
  """
  if data:
    if ES.indices.exists(index_name):
      ES.indices.delete(index=index_name)

    helpers.bulk(ES, data)
    ES.indices.refresh(index=index_name)

  set_last_process_date()

def get_last_process_date():
  try:
    with open(CTL_FILE, 'r') as f:
      line = f.readline()
      if not line == '':
        dt = line
      else:
        raise FileNotFoundError

  except FileNotFoundError:
    dt = time_in_minute(datetime.today(), 1).strftime("%Y-%m-%dT%H:%M:%S")

  return datetime.strptime(dt, "%Y-%m-%dT%H:%M:%S")

def set_last_process_date():
    with open(CTL_FILE, 'w') as f:
      f.writelines(time_in_minute(datetime.today(), 0).strftime("%Y-%m-%dT%H:%M:%S"))

def get_cursor_postgres():
    # connect to the PostgreSQL database
    conn = psycopg2.connect(host="192.168.56.190",database="capacity", user="elk", password='mokona69')
    # create a new cursor
    return  conn.cursor()

def get_thresholds():

    result = list()

    try:
        curs = get_cursor_postgres()

        curs.execute("SELECT name,value FROM elastalert_gui_threshold")
        
        result =[[row[0],row[1]] for row in curs.fetchall()] 
        
    except (Exception, psycopg2.DatabaseError) as error:
        print(error)
    finally:
        if curs is not None:
            curs.close()
    return result

def time_in_minute(dt, minus_time):
    if minus_time < 0:
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute) - timedelta( minutes= - minus_time)
    else:
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute) + timedelta( minutes= + minus_time)

if __name__=="__main__":      

    dt  = get_last_process_date()
    now = datetime.now()

    update_data = []

    for row in get_thresholds():

        while  dt < now:
            
            idx = {'_index': IDX}
            idx['_source'] = {}
            idx['_type'] = 'py_custom'
            idx['_source']['@timestamp'] = dt
            idx['_source']['@timestamp_import'] = now
            idx['_source']['name'] = row[0]
            idx['_source']['value'] = row[1]


            idx['_id'] = re.sub('[%s]| ' % re.escape(string.punctuation), '',

                'id_%s_%s' % (
                    idx['_source']['@timestamp'],
                    idx['_source']['name']
                )
            )

            update_data.append(idx)

            dt = time_in_minute(dt, 1)

    insert_ES(update_data)