#!/usr/bin/python
###################################################################################################
from datetime import datetime
from elasticsearch import Elasticsearch, helpers
from pytz import utc

es = Elasticsearch([{'host': 'elk_one'}])

search = es.search(index="filebeat-silk-log-*", body={
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


for project_loop in search['aggregations']['silk_project.keyword']['buckets']:

    update_data = list()

    idx_percent = 'python-silk-percent-'+str(project_loop['key']).lower()

    for script_loop in project_loop['silk_script.keyword']['buckets']:

        for transaction_loop in script_loop['silk_transaction.keyword']['buckets']:

            for segment_loop in transaction_loop['silk_script_segment.keyword']['buckets']:

                for release_loop in segment_loop['silk_script_release.keyword']['buckets']:

                    for measure_loop in release_loop['silk_measure.keyword']['buckets']:

                        idx_new = {}
                        idx_new['_index'] = idx_percent
                        idx_new['_type'] = 'py_custom'
                        idx_new['_doc_type'] = 'py_custom'
                        idx_new['_op_type'] = 'update'
                        idx_new['_source'] = {}
                        idx_new['_source']['doc'] = {}

                        timestamp = measure_loop['timestamp']['value_as_string']
                        result_ok_time = measure_loop['measure_time']['value']
                        result_ok_count = measure_loop['doc_count']

                        idx_new['_id'] = script_loop['key']+release_loop['key']+transaction_loop['key']+segment_loop['key']

                        idx_new['_source']['doc']['silk_script'] = script_loop['key']
                        idx_new['_source']['doc']['silk_script_release'] = release_loop['key']
                        idx_new['_source']['doc']['silk_transaction'] = transaction_loop['key']
                        idx_new['_source']['doc']['silk_script_segment'] = segment_loop['key']
                        idx_new['_source']['doc']['silk_measure'] = measure_loop['key']

                        idx_new['_source']['doc']['measure_time_ok'] = result_ok_time
                        idx_new['_source']['doc']['measure_count_ok'] = result_ok_count

                        idx_new['_source']['doc']['tags'] = '_custom_report'

                        idx_new['_source']['doc']['@timestamp'] = timestamp

                        idx_new['_source']['doc']['@timestamp'] = timestamp
                        idx_new['_source']['doc']['@timestamp_import'] = datetime.now(utc)

                        update_data.append(idx_new)

    helpers.bulk(es, update_data)
    es.indices.refresh(index=idx_percent)
