#######################################################################################################################
## # autor: Leandro Sampaio Silva
## # V 0.0.0
#######################################################################################################################

import copy
import io
import json
import re

from datetime import date, datetime, timedelta
from urllib.request import build_opener, HTTPBasicAuthHandler
from elasticsearch import Elasticsearch, helpers
from pytz import utc

#######################################################################################################################

DFT_IDX = 'python-ppt-data'

ES = Elasticsearch([{'host': '192.168.56.110'}])

ROOT_URL = 'http://192.168.56.190/capacidade'

CTL_FILE = 'integracao/.ctl_exec_data'

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

def time_in_minute(dt, minus_time):
  return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second) - timedelta( minutes=minus_time)

def time_with_tz(dt):
  return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute,tzinfo = timezone('America/Sao_Paulo')) 

def auth_handler():
  auth_handler = HTTPBasicAuthHandler()
  auth_handler.add_password(
    realm='django-tastypie', uri=ROOT_URL,
    user='lessilva', passwd='mokona69'
  )
  return build_opener(auth_handler)

def create_index(data, index_name=DFT_IDX):
  """
  """
  init_idx = {}
  init_idx['_type'] = 'py_custom'
  init_idx['_index'] = index_name
  init_idx['_source'] = data

  return init_idx

def remove_itens_from_dic(dict_del, lst_keys=['config_id', 'calcular_percentis', 'comparar_percentis', 'criacao', 'sistema_id'
                                              'metrica_externalid', 'componente_id', 'componente_externalid',
                                              'data', 'stats', 'resource_uri', 'pronto', 'id', 'tamanho', 'config_nome_interno' ]):
  for k in lst_keys:
      try:
        del dict_del[k]
      except KeyError:
        pass
  for v in dict_del.values():
    if isinstance(v, dict):
      remove_itens_from_dic(v, lst_keys)

  return dict_del
 
def insert_ES(data, index_name=DFT_IDX):
  """
  """
  if data:
    if ES.indices.exists(index_name):
      ES.indices.delete(index=index_name)

    helpers.bulk(ES, data)
    ES.indices.refresh(index=index_name)
    
  set_last_process_date()

#######################################################################################################################

def start_export():

  http = auth_handler()

  api_url = '{}/metricas/api/v1'.format(ROOT_URL)

  dt = get_last_process_date()
  
  with http.open(api_url + '/comparativo?criacao__gte={}'.format(dt)) as u, io.TextIOWrapper(u) as f:
    json_loaded = json.load(f)

  update_data = list()

  for summary_compare in json_loaded['objects']:

    id_snap_anterior = summary_compare['snapshot_anterior']['id']
    id_snap_atual    = summary_compare['snapshot_atual']['id']

    summary_compare['@timestamp'] = summary_compare['criacao']
    summary_compare['@timestamp_import'] = datetime.now(utc)

    with http.open('{}/comparativo/{}/result/'.format(api_url, summary_compare['id'])) as u, io.TextIOWrapper(u) as f:
      json_loaded = json.load(f)

    for kmetric, vmetric  in json_loaded.items():

      componente_id = vmetric['componente_id']
      metrica_id = kmetric

      with http.open('{}/snapshot/{}/data/{}/{}'.format(api_url, id_snap_anterior, componente_id, metrica_id)) as u, io.TextIOWrapper(u) as f1:
        data1 = json.load(f1)
      with http.open('{}/snapshot/{}/data/{}/{}'.format(api_url, id_snap_atual, componente_id, metrica_id)) as u, io.TextIOWrapper(u) as f2:
        data2 = json.load(f2)

      data1 = sorted(data1, key=lambda k: int(k[0]), reverse = False)
      data2 = sorted(data2, key=lambda k: int(k[0]), reverse = False)

      len_1 = len(data1)
      len_2 = len(data2)

      vmetric.update(summary_compare)
      remove_itens_from_dic(vmetric)

      count=0
      while count < max([len_1,len_2]):
        
        delta_date = data2[0][0] - data1[0][0]

        if  count < len_1:
              
          cp_metric = copy.deepcopy(vmetric)

          cp_metric["before"] = data1[count][1]
          cp_metric["@timestamp_relative"] = datetime.fromtimestamp(int(delta_date + data1[count][0])/1000)

          update_data.append( create_index( data=cp_metric))

        if  count < len_2:
              
          cp_metric = copy.deepcopy(vmetric)

          cp_metric["after" ] = data2[count][1]
          cp_metric["@timestamp_relative"]  = datetime.fromtimestamp(int(            data2[count][0])/1000)

          update_data.append( create_index( data=cp_metric))

        count+=1

  insert_ES(update_data)


if __name__ == '__main__':
    start_export()

