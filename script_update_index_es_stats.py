#######################################################################################################################
## # autor: Leandro Sampaio Silva
## # V 0.0.0
#######################################################################################################################

import copy
import io
import json
import re

import util

from datetime import date, datetime, timedelta
from urllib.request import build_opener, HTTPBasicAuthHandler
from elasticsearch import Elasticsearch, helpers
from pytz import utc

#######################################################################################################################

IDX = 'python-ppt-stats'

ES = Elasticsearch([{'host': '192.168.56.110'}])

ROOT_URL = 'http://192.168.56.190/capacidade'

#######################################################################################################################

def time_in_minute(dt, minus_time):
    return datetime(dt.year, dt.month, dt.day, dt.hour, dt.minute, dt.second) - timedelta( minutes=minus_time)

def auth_handler():
    auth_handler = HTTPBasicAuthHandler()
    auth_handler.add_password(
        realm='django-tastypie', uri=ROOT_URL,
        user='lessilva', passwd='mokona69'
    )
    return build_opener(auth_handler)

def remove_itens_from_dic(dict_del, lst_keys=['config_id', 'comparar_percentis', 'criacao', 'sistema_id', 
                                              'metrica_externalid', 'componente_id', 'resource_uri',
                                              'componente_externalid', 'pronto', 'data', 'id', 'tamanho' , 'config_nome_interno']):
    for k in lst_keys:
        try:
            del dict_del[k]
        except KeyError:
            pass
    for v in dict_del.values():
        if isinstance(v, dict):
            remove_itens_from_dic(v, lst_keys)

    return dict_del

#######################################################################################################################

def start_export():

    http = auth_handler()

    api_url = '{}/metricas/api/v1'.format(ROOT_URL)

    dt = util.get_last_process_date()

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

        data1 = sorted(data1, key=lambda k: int(k[1]), reverse = False)
        data2 = sorted(data2, key=lambda k: int(k[1]), reverse = False)

        len_1 = len(data1)
        len_2 = len(data2)

        vmetric.update(summary_compare)
        remove_itens_from_dic(vmetric)

        for kstat, vstat in vmetric['stats'].items():

            cp_metric = copy.deepcopy(vmetric)

            if re.match('p\d+',kstat):
                cp_metric['percentile'] = int(kstat[1:])
                cp_metric["@timestamp_relative"] = datetime.fromtimestamp(data2[int(len_2/100*int(kstat[1:]))][0]/1000)

            cp_metric.update(vstat)

            del cp_metric['stats']
            del cp_metric['calcular_percentis']
            idx = util.create_index(cp_metric,IDX)
            update_data.append(idx)

    util.insert_ES(update_data)

if __name__ == '__main__':
    start_export()
