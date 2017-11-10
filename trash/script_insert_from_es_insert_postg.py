#!/usr/bin/python
###########################################################################

import psycopg2
import psycopg2.extras

try:
    # connect to the PostgreSQL database
    conn = psycopg2.connect(host="192.168.56.190",database="capacity", user="elk", password='mokona69')
    
    # create a new cursor
    cur = conn.cursor()
    
    # execute the INSERT statement
    cur.execute("SELECT * FROM inventario_servidor")
    
    for row in cur.fetchall():
        print(row[1])

    # close communication with the database
    cur.close()
    # commit the changes to the database
    conn.close()
    
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None: conn.close()