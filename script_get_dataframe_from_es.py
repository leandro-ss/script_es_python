from elasticsearch import Elasticsearch
from elasticsearch.helpers import scan as es_scan
import jmespath
import pandas as pd

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
    
    res = pd.DataFrame(paths.search(list(es_scan(es, **query_kwargs))), columns=column_names)
    res = res.astype({'timestamp': 'datetime64[ms]'}).set_index(label_columns + timestamp_column).stack()
    res.index.levels[-1].rename('_metrica', inplace=True)
    res = res.rename('_value').reset_index()
    res = res.set_index([res.apply(lambda row: label_template.format(**row), axis=1).rename('label'), 'timestamp'])[['_value']]
    return res.sort_index()

es = Elasticsearch(['192.168.56.110'])

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
                    {"term":  {"silk_script_segment.keyword": "{{segment}}"}},
                    {"term":  {"silk_script_type.keyword":    "{{type}}"}},
                    {"term": {"silk_transaction_status.keyword": "Trans. ok[s]"}}
                ],
                "must_not": [
                    {"term": {"silk_transaction.keyword": "TInit"}},
                ]
            }
        },
        "docvalue_fields": [
            "@timestamp", "silk_script.keyword", "silk_transaction.keyword",
            "silk_transaction_status.keyword", "silk_transaction_time"
        ]
    },
    "params": {
        "start_time": "2017-06-01T00:00:00Z",
        "end_time": "2017-06-02T00:00:00Z",
        "segment": "Private",
        "type": "Conta Corrente",
    }
}

params = {
    "start_time": "2017-06-01T00:00:00Z",
    "end_time": "2017-06-02T00:00:00Z",
    "segment": "Private",
    "type": "Conta Corrente",
}

column_mapping = {
    'timestamp': 'fields."@timestamp"[0]',
    'label': {
        'template': '{script}/{transaction} ({status})',
        'columns': {
            'script': 'fields."silk_script.keyword"[0]',
            'transaction': 'fields."silk_transaction.keyword"[0]',
            'status': 'fields."silk_transaction_status.keyword"[0]',
        }
    },
    'values': {
        'transaction_time': 'fields.silk_transaction_time[0]'
    }
}

df = run_query(es, template, params, column_mapping, index='filebeat-*', filter_path=['hits.hits.fields'])
