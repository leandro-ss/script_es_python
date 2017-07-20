#!/usr/bin/python
from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])

res = es.search(index="filebeat-access-*", body={
    "aggs" :{
        "group_by"       : { "terms" : { "field" : "@timestamp" },
        "aggs"           : {  "min_time_taken" : { "min" : { "field" : "time_taken" }},
                              "P90_time_taken" : { "percentiles" : { "field" : "time_taken",  "percents" : [90] } },
                              "max_time_taken" : { "max" : { "field" : "time_taken" } }}
        }
    }
})

import psycopg2
import psycopg2.extras

try:
    psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

# connect to the PostgreSQL database
    conn = psycopg2.connect(host="localhost",database="elk", user="elk")
    
    # create a new cursor
    cur = conn.cursor()
    
    # execute the INSERT statement
    for hit in res["aggregations"]["group_by"]["buckets"]:
        cur.execute("INSERT INTO orders (info) VALUES (%s)", (hit,))
    #args_str = ','.join(cur.mogrify("(%s)", x) for x in res["aggregations"]["group_by"]["buckets"])
    #cur.execute("INSERT INTO orders (info) VALUES " + args_str) 

    #insert_query = 'INSERT INTO orders (info) VALUES %s',
    #psycopg2.extras.execute_values ( cur, insert_query, res["aggregations"]["group_by"]["buckets"], template=None, page_size=100 )

    # close communication with the database
    cur.close()
    # commit the changes to the database
    conn.commit()
    # close communication with the database
    conn.close()
    
except (Exception, psycopg2.DatabaseError) as error:
    print(error)
finally:
    if conn is not None: conn.close()