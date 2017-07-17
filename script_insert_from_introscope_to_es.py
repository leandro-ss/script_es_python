#!/usr/bin/python
################################################################################################################
## # https://stackoverflow.com/questions/26371237/reindexing-elastic-search-via-bulk-api-scan-and-scroll
## # https://stackoverflow.com/questions/32285596/elasticsearch-python-re-index-data-after-changing-the-mappings
##
## # autor: Leandro Sampaio Silva
## # Objetivo: Upload dos percentuais a serem considerados durante a analise comparativa dos releases do Silk
## # V 0.0.0  
################################################################################################################
import jaydebeapi

class Cursor_Extent (jaydebeapi.Cursor):
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

from pytz     import utc, timezone
from datetime import date, datetime, timedelta

def time_in_minute(dt, minus_time):
    return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute) - timedelta( minutes=minus_time)

def time_with_tz(dt):
    return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute,tzinfo = timezone('America/Sao_Paulo')) 


conn = jaydebeapi.connect("com.wily.introscope.jdbc.IntroscopeDriver",
                          "jdbc:introscope:net//Inmetrics:@localhost:5432", ["",""],
                          "C:\dev\workspace_python\lib\IntroscopeJDBC.jar",)

curs = Cursor_Extent(conn, jaydebeapi._converters )

dt_ini = time_in_minute(datetime.today(), 1).strftime("%m/%d/%Y %H:%M:%S")
dt_end = time_in_minute(datetime.today(), 0).strftime("%m/%d/%Y %H:%M:%S")

sql = "select * from metric_data where agent = '.*SHWT060CTO.*'  and metric='.*' and timestamp between '"+dt_ini+"' and '"+dt_end+"'"

curs.execute_statement(sql)

update_data = []

IDX = 'python-jdbc-introscope' + date.today().strftime("-%Y-%m-%d");

for row in curs.fetchall():
    
    idx_new = {'_index': IDX}
    
    idx_new['_source'] = {}; idx_new['_type'] = 'py_custom';
    idx_new['_source']['@timestamp'] = time_with_tz(datetime.strptime(row[10], '%Y-%m-%d %H:%M:%S'))
    idx_new['_source']['@timestamp_import'] = datetime.now(utc)
    idx_new['_source']['domain'] = row[0]
    idx_new['_source']['host'] = row[1]
    idx_new['_source']['process'] = row[2]
    idx_new['_source']['resource'] = row[4]
    idx_new['_source']['value'] = row[13]
    
    update_data.append(idx_new)
    
curs.close()
conn.close()

from elasticsearch import Elasticsearch,helpers
es = Elasticsearch([{'host': '10.31.75.70', 'port': 9200}])

helpers.bulk(es,update_data)

es.indices.refresh(index= IDX)