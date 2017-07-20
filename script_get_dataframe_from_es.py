from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan as es_scan
import jmespath
import pandas as pd

es = Elasticsearch(['localhost'])

template = {
    "inline": {
        "_source": False,
        "query": {
            "bool": {
                "filter": [
                    {"range": {"@timestamp": {
                                "gte":                 "{{start_time}}",
                                "le":                  "{{end_time}}"   }}},
                    {"term":  {"tags":                 "_sumary_report" }},
                    {"term":  {"silk_project.keyword": "{{projeto}}"    }}
                ]
            }
        },
        "docvalue_fields": [
            "@timestamp", "user_profile.keyword",
            "silk_script.keyword", "silk_script_segment.keyword",
            "silk_script_type.keyword", "silk_transaction_time",
            "time_action"
        ]
    },
    "params": {
        "start_time": "2017-06-01T19:00:00Z",
        "end_time": "2017-06-01T20:00:00Z",
        "projeto": "Projeto"
    }
}

params = {
    "start_time": "2017-06-01T00:00:00Z",
    "end_time": "2017-06-02T00:00:00Z",
    "projeto": "Projeto"
}

column_mapping = {
    'timestamp': 'fields."@timestamp"[0]',
    'label': {
        'template': '{profile}/{segment}.{type}/{script} ({_metrica})',
        'columns': {
            'profile': 'fields."user_profile.keyword"[0]',
            'segment': 'fields."silk_script_segment.keyword"[0]',
            'type': 'fields."silk_script_type.keyword"[0]',
            'script': 'fields."silk_script.keyword"[0]'
        }
    },
    'values': {
        'transaction_time': 'fields.silk_transaction_time[0]',
        'time_action': 'fields.time_action[0]',
    }
}

def run_query(es, template, params, column_mapping, **query_kwargs):
    timestamp_column = ['timestamp']
    label_columns = list(column_mapping['label']['columns'].keys())
    metric_columns = list(column_mapping['values'].keys())
    column_names = timestamp_column + label_columns + metric_columns
    column_paths = [column_mapping['timestamp']] + \
        [column_mapping['label']['columns'][k] for k in label_columns] + \
        [column_mapping['values'][k] for k in metric_columns]
    paths = jmespath.compile('[].[' + ', '.join(column_paths) + ']')
    label_template = column_mapping['label']['template']
    
    body = dict(template)
    if params:
        body['params'] = params
    query_kwargs['query'] = es.render_search_template(body=body)['template_output']
    
    filter_path = query_kwargs.pop('filter_path', None)
    if filter_path:
        query_kwargs['filter_path'] = list(set(filter_path + ['_scroll_id', '_shards']))
    
    result = es_scan(es, **query_kwargs)
    
    result = list(result)
    
    pat = paths.search(result)
    
    res = pd.DataFrame(pat , columns=column_names)
    res = res.astype({'timestamp': 'datetime64[ms]'}).set_index(label_columns + timestamp_column).stack()
    res.index.levels[-1].rename('_metrica', inplace=True)
    res = res.rename('_value').reset_index()
    res = res.set_index([res.apply(lambda row: label_template.format(**row), axis=1).rename('label'), 'timestamp'])[['_value']]
    return res.sort_index()

df = run_query(es, template, params, column_mapping, index='filebeat-*', filter_path=['hits.hits.fields'])
