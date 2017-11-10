##################################################################################################################
## # autor: Leandro Sampaio Silva
## # V 0.0.0
################################################################################################################
import re
import jaydebeapi
import psycopg2
import psycopg2.extras
from pytz     import utc, timezone
from datetime import date, datetime, timedelta
from elasticsearch import Elasticsearch,helpers

#######################################################################################################################

ES = Elasticsearch([{"host": "192.168.56.110"}])

BASE = "host=192.168.56.190 database=capacity  user=elk password=mokona69"

IDX = "python-jdbc-introscope" + date.today().strftime("-%Y-%m-%d")

METRIC = "Frontends\\|Apps\\|([A-z -]*):"+\
         "("+\
         "Responses Per Interval|"+\
         "Average Response Time \\(ms\\)|"+\
         "Stall Count|"+\
         "Errors Per Interval|"+\
         ")"

CTL_FILE = "integracao/.ctl_exec_scope"

#######################################################################################################################

def get_last_process_date():
  try:

    with open(CTL_FILE, 'r') as f:
      line = f.readline()
      if not line == '':
        dt = line
      else:
        raise FileNotFoundError

  except FileNotFoundError:
    dt = time_in_minute(datetime.today(), 1).strftime("%Y-%m-%d %H:%M:%S").replace(" ", "%20")

  return dt

def set_last_process_date():
    with open(CTL_FILE, 'w') as f:
      f.writelines(time_in_minute(datetime.today(), 0).strftime("%Y-%m-%d %H:%M:%S").replace(" ", "%20"))

def get_cursor_postgres():
    # connect to the PostgreSQL database
    conn = psycopg2.connect(BASE)
    # create a new cursor
    return  conn.cursor()

def get_cursor_introscope():
    # connect to introscope database
    conn = jaydebeapi.connect("com.wily.introscope.jdbc.IntroscopeDriver",
                            "jdbc:introscope:net//Inmetrics:@10.58.78.211:5010", ["",""],
                            "/opt/perfcenter/portal-capacidade/integracao/driver/IntroscopeJDBC.jar",)
    # create a new cursor
    return Cursor_Extend(conn, jaydebeapi._converters)

def get_list_server():

    result = list()
    try:
        curs = get_cursor_postgres()

        # execute the INSERT statement
        curs.execute("SELECT hostname FROM inventario_servidor")
        
        result.append(row[1] for row in cur.fetchall())
        
    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if curs is not None:
            curs.close()

    return result

def time_in_minute(dt, minus_time):
    if minus_time < 0:
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute) - timedelta( minutes= - minus_time)
    else:
        return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute) + timedelta( minutes= + minus_time)

def time_with_tz(dt):
    return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute,tzinfo = timezone('America/Sao_Paulo')) 

## Extends class to support a common statement
class Cursor_Extend (jaydebeapi.Cursor):
    def execute_statement(self, operation, parameters=None):
        if self._connection._closed:
            raise jaydebeapi.Error()
        if not parameters:
            parameters = ()
        self._close_last()
        self._prep = self._connection.jconn.createStatement()
    
        try:
            is_rs = self._prep.executeQuery(operation)
        except:
            self._handle_sql_exception()
            
        if is_rs:
            self._rs = self._prep.getResultSet()
            self._meta = self._rs.getMetaData()
            self.rowcount = -1
        else:
            jaydebeapi.rowcount = self._prep.getUpdateCount()

if __name__=="__main__":

    dt  = get_last_process_date()
    now = datetime.now(utc)

    metric = METRIC

    update_data = []

    try:
        for hostname in get_list_server():

            curs = get_cursor_introscope()

            dt_ini = time_in_minute(dt, -1).strftime("%m/%d/%Y %H:%M:%S")
            dt_end = time_in_minute(dt,  0).strftime("%m/%d/%Y %H:%M:%S")

            sql = "SELECT * FROM metric_data where agent = '.*"+hostname+".*' AND metric='"+metric+"' AND timestamp BETWEEN '"+dt_ini+"' AND '"+dt_end+"'"

            curs.execute_statement(sql)

            for row in curs.fetchall():
            
                idx = {'_index': IDX}


                idx['_type'] = 'py_custom'
                idx['_source'] = {}
                idx['_source']['@timestamp'] = dt_ini
                idx['_source']['@timestamp_import'] = datetime.now(utc)
                idx['_source']['domain'] = row[0]
                idx['_source']['hosthame'] = row[1]
                idx['_source']['process'] = row[2]
                idx['_source']['resource'] = row[4]
                idx['_source']['value'] = row[13]

                idx['_id'] = re.replace('[[:punct:]]', 

                    "id_%s_%s_%s" % (
                        idx['_source']['@timestamp'],
                        idx['_source']['process'],
                        idx['_source']['resource']
                    )
                )

            update_data.append(idx)

            dt = time_in_minute(dt, 1)

    except (Exception, jaydebeapi.DatabaseError) as error:
        raise error
    finally:
        if curs is not None:
            curs.close()

    helpers.bulk(ES,update_data)
    ES.indices.refresh(index= IDX)

    set_last_process_date()