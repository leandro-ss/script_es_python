##################################################################################################################
## # autor: Leandro Sampaio Silva
## # V 0.0.0
################################################################################################################

from datetime import date, datetime
from pytz     import utc

import psycopg2
import psycopg2.extras

import util

#######################################################################################################################

IDX = 'python-test' + date.today().strftime('-%Y-%m-%d')

#######################################################################################################################

def get_thresholds():
    """
    """
    curs = util.get_cursor_postgres()
    try:        
        curs.execute("SELECT name,value FROM elastalert_gui_threshold")
        result =[[row[0],row[1]] for row in curs.fetchall()] 
        
    except (Exception, psycopg2.DatabaseError) as error:
        result = list()

    finally:
        if curs is not None:
            curs.close()
    return result

def start_process():
    """
    """
    dt  = util.get_last_process_date()
    now = datetime.now(utc)

    update_data = []

    for row in get_thresholds():

        while  dt < now:

            data = {}
            data['@timestamp'] = dt
            data['name'] = row[0]
            data['value'] = row[1]

            idx_id = util.get_index_id(

                data['@timestamp'],
                data['name']
            )

            update_data.append(util.create_index(data, IDX, idx_id))

            dt = util.time_in_minute(dt, 1)

    util.insert_ES(update_data)

if __name__=="__main__":
    start_process()