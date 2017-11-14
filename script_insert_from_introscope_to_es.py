##################################################################################################################
## # autor: Leandro Sampaio Silva
## # V 0.0.0
################################################################################################################
import jaydebeapi
import util
import psycopg2
import psycopg2.extras
from pytz     import utc
from datetime import date, datetime

#######################################################################################################################

IDX = "python-jdbc-introscope" + date.today().strftime("-%Y-%m-%d")

METRIC = "Frontends\\|Apps\\|([A-z -]*):"+\
         "("+\
         "Responses Per Interval|"+\
         "Average Response Time \\(ms\\)|"+\
         "Stall Count|"+\
         "Errors Per Interval|"+\
         ")"

#######################################################################################################################

def get_cursor_introscope():
    """ connect to the PostgreSQL database, create a new cursor
    """
    conn = jaydebeapi.connect(
        "com.wily.introscope.jdbc.IntroscopeDriver",
        "jdbc:introscope:net//Inmetrics:@10.58.78.211:5010", ["",""],
        "/opt/perfcenter/portal-capacidade/integracao/driver/IntroscopeJDBC.jar",
    )
    return Cursor_Extend(conn, jaydebeapi._converters)

class Cursor_Extend (jaydebeapi.Cursor):
    """ Extends class to support a common statement
    """
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

def get_list_server():

    try:
        curs = util.get_cursor_postgres()
        curs.execute("SELECT hostname FROM inventario_servidor")
        result = [row[1] for row in curs.fetchall()]
        
    except (Exception, psycopg2.DatabaseError) as error:
        raise error
    finally:
        if curs is not None:
            curs.close()

    return result

def start_process():
    """
    """

    dt_process  = util.get_last_process_date()
    now = datetime.now(utc)

    update_data = []

    try:

        curs = get_cursor_introscope()

        while  dt_process < now:

            for hostname in get_list_server():

                dt_ini = util.datetime_utc2local(util.time_in_minute(dt, -1)).strftime("%m/%d/%Y %H:%M:%S")
                dt_end = util.datetime_utc2local(util.time_in_minute(dt,  0)).strftime("%m/%d/%Y %H:%M:%S")

                sql = "SELECT * FROM metric_data where agent = '.*"+hostname+".*' AND metric='"+METRIC+"' AND timestamp BETWEEN '"+dt_ini+"' AND '"+dt_end+"'"

                curs.execute_statement(sql)

                for row in curs.fetchall():
                
                    data = {}
                    data['@timestamp'] = dt_ini
                    data['domain'] = row[0]
                    data['hosthame'] = row[1]
                    data['process'] = row[2]
                    data['resource'] = row[4]
                    data['value'] = row[13]

                    idx_id = util.get_index_id(

                        data['@timestamp'],
                        data['process'],
                        data['resource']
                    )

                    update_data.append(util.create_index(data,IDX,idx_id))

            dt_process = util.time_in_minute(dt_process, 1)

    except (Exception, jaydebeapi.DatabaseError) as error:
        raise error
    finally:
        if curs is not None:
            curs.close()

    util.insert_ES(update_data)
    

if __name__=="__main__":
    """
    """
    start_process()