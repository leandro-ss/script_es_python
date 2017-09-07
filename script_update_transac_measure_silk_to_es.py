#!/usr/bin/python
###################################################################################################
import re
from datetime import datetime
from decimal import Decimal
from elasticsearch import Elasticsearch, helpers
from pytz import utc

es = Elasticsearch([{'host': 'elk_one'}])


def create_index(index_name):
    """
    """
    init_idx = {}
    init_idx['_index'] = index_name
    init_idx['_type'] = 'py_custom'
    init_idx['_type_op'] = 'upsert'
    init_idx['_source'] = {}

    return init_idx

def estimate_percent(base, time):
    """
    """
    return trunc_value(((base/time) -1)*100)

def trunc_value(value):
    """
    """
    return float(round(Decimal(value), 2))


search_1 = es.search(index="filebeat-silk-log-*", body={
    "from" : 0,
    "size" : 0,
    "_source" : {
        "includes" : [
            "_index",
            "silk_project.keyword",
            "silk_script_release.keyword"
        ],
        "excludes" : []
    },
    "stored_fields" : [
        "_index",
        "silk_project.keyword",
        "silk_script_release.keyword"
    ],
    "sort" : [
        {
            "_index" : {
                "order" : "desc"
            }
        }
    ],
    "aggregations" : {
        "_index" : {
            "terms" : {
                "field" : "_index",
                "size" : 200,
                "min_doc_count" : 1,
                "shard_min_doc_count" : 0,
                "show_term_doc_count_error" : False,
                "order" : {
                    "_term" : "desc"
                }
            },
            "aggregations" : {
                "silk_project.keyword" : {
                    "terms" : {
                        "field" : "silk_project.keyword",
                        "size" : 200,
                        "min_doc_count" : 1,
                        "shard_min_doc_count" : 0,
                        "show_term_doc_count_error" : False,
                        "order" : {
                            "_term" : "desc"
                        }
                    },
                    "aggregations" : {
                        "silk_script_release.keyword" : {
                            "terms" : {
                                "field" : "silk_script_release.keyword",
                                "size" : 10,
                                "min_doc_count" : 1,
                                "shard_min_doc_count" : 0,
                                "show_term_doc_count_error" : False,
                                "order" : [
                                    {
                                        "_count" : "desc"
                                    },
                                    {
                                        "_term" : "asc"
                                    }
                                ]
                            }
                        }
                    }
                }
            }
        }
    }
})

search_2 = es.search(index="filebeat-silk-log-*", body={
    "from" : 0,
    "size" : 0,
    "query" : {
        "bool" : {
            "filter" : [
                {
                    "bool" : {
                        "must" : [
                            {
                                "bool" : {
                                    "should" : [
                                        {
                                            "match_phrase" : {
                                                "silk_transaction_status.keyword" : {
                                                    "query" : "Trans. ok[s]",
                                                    "boost" : 1.0
                                                }
                                            }
                                        },
                                        {
                                            "match_phrase" : {
                                                "silk_transaction_status.keyword" : {
                                                    "query" : "Trans. failed[s]",
                                                    "boost" : 1.0
                                                }
                                            }
                                        }
                                    ],
                                    "disable_coord" : False,
                                    "adjust_pure_negative" : True,
                                    "boost" : 1.0
                                }
                            },
                            { "match" : { "silk_script.keyword" : "16_Trn_TEDMinhasContas" }},#TESTE
                        ],
                        "disable_coord" : False,
                        "adjust_pure_negative" : True,
                        "boost" : 1.0
                    }
                }
            ],
            "disable_coord" : False,
            "adjust_pure_negative" : True,
            "boost" : 1.0
        }
    },
    "_source": False,
    "docvalue_fields": [
        "silk_project.keyword",
        "silk_script.keyword",
        "silk_transaction.keyword",
        "silk_script_segment.keyword",
        "silk_script_release.keyword",
        "silk_transaction_status.keyword",
        "AVG",
        "MIN"
    ],
    "stored_fields" : [
        "silk_project.keyword",
        "silk_script.keyword",
        "silk_transaction.keyword",
        "silk_script_segment.keyword",
        "silk_script_release.keyword",
        "silk_transaction_status.keyword"
    ],
    "aggregations" : {
        "silk_project.keyword" : {
            "terms" : {
                "field" : "silk_project.keyword",
                "size" : 200,
                "min_doc_count" : 1,
                "shard_min_doc_count" : 0,
                "show_term_doc_count_error" : False
            },
            "aggregations" : {
                "silk_script.keyword" : {
                    "terms" : {
                        "field" : "silk_script.keyword",
                        "size" : 200,
                        "min_doc_count" : 1,
                        "shard_min_doc_count" : 0,
                        "show_term_doc_count_error" : False
                    },
                    "aggregations" : {
                        "silk_transaction.keyword" : {
                            "terms" : {
                                "field" : "silk_transaction.keyword",
                                "size" : 10,
                                "min_doc_count" : 1,
                                "shard_min_doc_count" : 0,
                                "show_term_doc_count_error" : False
                            },
                            "aggregations" : {
                                "silk_script_segment.keyword" : {
                                    "terms" : {
                                        "field" : "silk_script_segment.keyword",
                                        "size" : 10,
                                        "min_doc_count" : 1,
                                        "shard_min_doc_count" : 0,
                                        "show_term_doc_count_error" : False
                                    },
                                    "aggregations" : {
                                        "silk_script_release.keyword" : {
                                            "terms" : {
                                                "field" : "silk_script_release.keyword",
                                                "size" : 10,
                                                "min_doc_count" : 1,
                                                "shard_min_doc_count" : 0,
                                                "show_term_doc_count_error" : False
                                            },
                                            "aggregations" : {
                                                "silk_transaction_status.keyword" : {
                                                    "terms" : {
                                                        "field" : "silk_transaction_status.keyword",
                                                        "size" : 10,
                                                        "min_doc_count" : 1,
                                                        "shard_min_doc_count" : 0,
                                                        "show_term_doc_count_error" : False
                                                    },
                                                    "aggregations" : {
                                                        "result_time" : {
                                                            "avg" : {
                                                                "field" : "silk_transaction_time"
                                                            }
                                                        },
                                                        "timestamp" : {
                                                            "min" : {
                                                                "field" : "@timestamp"
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
})

search_3 = es.search(index="filebeat-silk-log-*", body={
    "from" : 0,
    "size" : 0,
    "_source" : {
        "includes" : [
            "silk_project.keyword",
            "silk_script.keyword",
            "silk_transaction.keyword",
            "silk_script_segment.keyword",
            "silk_script_release.keyword",
            "silk_measure.keyword",
            "AVG",
            "MIN"
        ],
        "excludes" : []
    },
    "stored_fields" : [
        "silk_project.keyword",
        "silk_script.keyword",
        "silk_transaction.keyword",
        "silk_script_segment.keyword",
        "silk_script_release.keyword",
        "silk_measure.keyword"
    ],
    "aggregations" : {
        "silk_project.keyword" : {
            "terms" : {
                "field" : "silk_project.keyword",
                "size" : 200,
                "min_doc_count" : 1,
                "shard_min_doc_count" : 0,
                "show_term_doc_count_error" : False,
                "order" : [
                    {
                        "_count" : "desc"
                    },
                    {
                        "_term" : "asc"
                    }
                ]
            },
            "aggregations" : {
                "silk_script.keyword" : {
                    "terms" : {
                        "field" : "silk_script.keyword",
                        "size" : 10,
                        "min_doc_count" : 1,
                        "shard_min_doc_count" : 0,
                        "show_term_doc_count_error" : False,
                        "order" : [
                            {
                                "_count" : "desc"
                            },
                            {
                                "_term" : "asc"
                            }
                        ]
                    },
                    "aggregations" : {
                        "silk_transaction.keyword" : {
                            "terms" : {
                                "field" : "silk_transaction.keyword",
                                "size" : 10,
                                "min_doc_count" : 1,
                                "shard_min_doc_count" : 0,
                                "show_term_doc_count_error" : False,
                                "order" : [
                                    {
                                        "_count" : "desc"
                                    },
                                    {
                                        "_term" : "asc"
                                    }
                                ]
                            },
                            "aggregations" : {
                                "silk_script_segment.keyword" : {
                                    "terms" : {
                                        "field" : "silk_script_segment.keyword",
                                        "size" : 10,
                                        "min_doc_count" : 1,
                                        "shard_min_doc_count" : 0,
                                        "show_term_doc_count_error" : False,
                                        "order" : [
                                            {
                                                "_count" : "desc"
                                            },
                                            {
                                                "_term" : "asc"
                                            }
                                        ]
                                    },
                                    "aggregations" : {
                                        "silk_script_release.keyword" : {
                                            "terms" : {
                                                "field" : "silk_script_release.keyword",
                                                "size" : 10,
                                                "min_doc_count" : 1,
                                                "shard_min_doc_count" : 0,
                                                "show_term_doc_count_error" : False,
                                                "order" : [
                                                    {
                                                        "_count" : "desc"
                                                    },
                                                    {
                                                        "_term" : "asc"
                                                    }
                                                ]
                                            },
                                            "aggregations" : {
                                                "silk_measure.keyword" : {
                                                    "terms" : {
                                                        "field" : "silk_measure.keyword",
                                                        "size" : 10,
                                                        "min_doc_count" : 1,
                                                        "shard_min_doc_count" : 0,
                                                        "show_term_doc_count_error" : False,
                                                        "order" : [
                                                            {
                                                                "_count" : "desc"
                                                            },
                                                            {
                                                                "_term" : "asc"
                                                            }
                                                        ]
                                                    },
                                                    "aggregations" : {
                                                        "measure_time" : {
                                                            "avg" : {
                                                                "field" : "silk_measure_time"
                                                            }
                                                        },
                                                        "timestamp" : {
                                                            "min" : {
                                                                "field" : "@timestamp"
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
})

project_dct = dict()

for index_loop in search_1['aggregations']['_index']['buckets']:

    for project_loop in index_loop['silk_project.keyword']['buckets']:

        for release_loop in project_loop['silk_script_release.keyword']['buckets']:

            if  project_loop['key'] in project_dct:
                if release_loop['key'] not in project_dct[project_loop['key']]:
                    project_dct[project_loop['key']].append(release_loop['key'])
            else:
                project_dct[project_loop['key']] = list()
                project_dct[project_loop['key']].append(release_loop['key'])

for project_loop in search_2['aggregations']['silk_project.keyword']['buckets']:

    update_idx = list()
    result_idx = list()

    idx_name = 'python-silk-percent-'+str(project_loop['key']).lower()

    idx_pst = project_dct[project_loop['key']]

    baseline = next((release for release in idx_pst if re.match("^Baseline",release)), None)

    while len(idx_pst) > 0:

        for script_loop in project_loop['silk_script.keyword']['buckets']:

            for transaction_loop in script_loop['silk_transaction.keyword']['buckets']:

                for segment_loop in transaction_loop['silk_script_segment.keyword']['buckets']:

                    if any(idx_pst[0] == dic['key'] for dic in segment_loop['silk_script_release.keyword']['buckets']):

                        idx = create_index(idx_name)

                        for release_loop in segment_loop['silk_script_release.keyword']['buckets']:

                            result_ok_time = float(0)
                            result_nok_time = float(0)
                            result_nok_count = int(0)
                            result_ok_count = int(0)

                            timestamp_start = ''

                            for status_loop in release_loop['silk_transaction_status.keyword']['buckets']:

                                timestamp = status_loop['timestamp']['value_as_string']

                                if status_loop['key'] == 'Trans. ok[s]':

                                    result_ok_time = trunc_value(status_loop['result_time']['value'])
                                    result_ok_count = status_loop['doc_count']

                                elif status_loop['key'] == 'Trans. failed[s]':

                                    result_nok_time = trunc_value(status_loop['result_time']['value'])
                                    result_nok_count = status_loop['doc_count']

                            if idx_pst[0] == release_loop['key']:

                                idx['_id'] = script_loop['key']+release_loop['key']+transaction_loop['key']+segment_loop['key']

                                idx['_source']['silk_script'] = script_loop['key']

                                idx['_source']['silk_projetc'] = project_loop['key']
                                idx['_source']['silk_script_release'] = release_loop['key']
                                idx['_source']['silk_transaction'] = transaction_loop['key']
                                idx['_source']['silk_script_segment'] = segment_loop['key']

                                idx['_source']['trasaction_time_ok'] = result_ok_time
                                idx['_source']['trasaction_count_nok'] = result_nok_count
                                idx['_source']['trasaction_count_ok'] = result_ok_count

                                idx['_source']['@timestamp'] = timestamp
                                idx['_source']['@timestamp_import'] = datetime.now(utc)

                            if baseline == release_loop['key']:
                                idx['_source']['transaction_baseline'] = result_ok_time
                            elif 'transaction_baseline' not in idx['_source']:
                                idx['_source']['transaction_baseline'] = float(0)

                        if idx['_source'].get('trasaction_time_ok') and idx['_source'].get('transaction_baseline'):
                            idx['_source']['percent_baseline'] = estimate_percent(idx['_source']['transaction_baseline'], idx['_source']['trasaction_time_ok'])
                        else:
                            idx['_source']['percent_baseline'] = float(0)

                        update_idx.append(idx)
        idx_pst.pop(0)

    measure_prt_loop = next((next_project for next_project in search_3['aggregations']['silk_project.keyword']['buckets'] if next_project['key'] == project_loop['key']), None)

    for script_loop in measure_prt_loop['silk_script.keyword']['buckets']:

        for transaction_loop in script_loop['silk_transaction.keyword']['buckets']:

            for segment_loop in transaction_loop['silk_script_segment.keyword']['buckets']:

                for release_loop in segment_loop['silk_script_release.keyword']['buckets']:

                    key = script_loop['key']+release_loop['key']+transaction_loop['key']+segment_loop['key']

                    baseline = next((release for release in idx_pst if re.compile("^Baseline").match(release)), None)

                    for idx in update_idx:

                        if key == idx['_id']:

                            for measure_loop in release_loop['silk_measure.keyword']['buckets']:
                                idx['_id'] = script_loop['key']+release_loop['key']+transaction_loop['key']+segment_loop['key']+measure_loop['key']

                                idx['_source']['silk_measure'] = measure_loop['key']
                                idx['_source']['measure_time_ok'] = measure_loop['measure_time']['value']
                                idx['_source']['measure_count_ok'] = measure_loop['doc_count']
                                result_idx.append(idx)

                        elif not idx['_source'].get('silk_measure'):

                            idx['_source']['silk_measure'] = 'n/a'
                            idx['_source']['measrure_time_ok'] = float(0)
                            idx['_source']['measure_count_ok'] = int(0)
                            result_idx.append(idx)

    helpers.bulk(es, result_idx)
    es.indices.refresh(index=idx_name)
