#!/usr/bin/python
################################################################################################################
## # https://stackoverflow.com/questions/26371237/reindexing-elastic-search-via-bulk-api-scan-and-scroll
## # https://stackoverflow.com/questions/32285596/elasticsearch-python-re-index-data-after-changing-the-mappings
##
## # autor: Leandro Sampaio Silva
## # Objetivo: Upload dos percentuais a serem considerados durante a analise comparativa dos releases do Silk
## # V 0.0.0  
################################################################################################################

from datetime import date,datetime, timedelta

def previous_month(dt):
   return datetime(dt.year, dt.month, dt.day) - timedelta(days=1)
#  return datetime(dt.year, dt.month, 1) - timedelta(days=1)

from elasticsearch import Elasticsearch

es = Elasticsearch([{'host': 'localhost', 'port': 9200}])
   
for index in es.indices.get('*'):
    if previous_month(date.today()).strftime("%Y.%m.%d") in index:
        print(es.indices.delete(index))