import io
import json
import os
from urllib.request import build_opener, HTTPBasicAuthHandler

root_url = 'http://ec2-54-207-203-38.sa-east-1.compute.amazonaws.com/capacidade'
username = 'lessilva'
password = 'mokona69'

auth_handler = HTTPBasicAuthHandler()
auth_handler.add_password(
    realm='django-tastypie', uri=root_url,
    user=username, passwd=password
)
http = build_opener(auth_handler)

api_url = '{}/metricas/api/v1'.format(root_url)

with http.open(api_url + '/comparativo?criacao__gte=2017-08-01') as u, io.TextIOWrapper(u) as f:
    comparativos = json.load(f)

with open('comparativo.json', 'w') as f:
    json.dump(comparativos, f)

for c in comparativos['objects']:
    print('comparativo {}'.format(c['id']))
    print('..result')
    
    with http.open('{}/comparativo/{}/result/'.format(api_url, c['id'])) as u, io.TextIOWrapper(u) as f:
        result = json.load(f)
    
    os.makedirs('comparativo/{}'.format(c['id']), exist_ok=True)
    with open('comparativo/{}/result.json'.format(c['id']), 'w') as f:
        json.dump(result, f)
    
    for metrica_id, metrica in result.items():
        componente_id = metrica['componente_id']
        
        print('..metrica {} {}/{}/{}'.format(metrica['metrica_nome'], c['snapshot_anterior']['id'], componente_id, metrica_id))
        
        with http.open('{}/snapshot/{}/data/{}/{}'.format(api_url, c['snapshot_anterior']['id'], componente_id, metrica_id)) as u, io.TextIOWrapper(u) as f:
            data = json.load(f)
        
        os.makedirs('snapshot/{}/data/{}'.format(c['snapshot_anterior']['id'], componente_id), exist_ok=True)
        with open('snapshot/{}/data/{}/{}.json'.format(c['snapshot_anterior']['id'], componente_id, metrica_id), 'w') as f:
            json.dump(data, f)
        
        print('..metrica {} {}/{}/{}'.format(metrica['metrica_nome'], c['snapshot_atual']['id'], componente_id, metrica_id))
        
        with http.open('{}/snapshot/{}/data/{}/{}'.format(api_url, c['snapshot_atual']['id'], componente_id, metrica_id)) as u, io.TextIOWrapper(u) as f:
            data = json.load(f)
        
        os.makedirs('snapshot/{}/data/{}'.format(c['snapshot_atual']['id'], componente_id), exist_ok=True)
        with open('snapshot/{}/data/{}/{}.json'.format(c['snapshot_atual']['id'], componente_id, metrica_id), 'w') as f:
            json.dump(data, f)
