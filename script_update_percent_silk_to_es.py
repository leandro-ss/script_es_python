################################################################################################################
## # https://stackoverflow.com/questions/26371237/reindexing-elastic-search-via-bulk-api-scan-and-scroll
## # https://stackoverflow.com/questions/32285596/elasticsearch-python-re-index-data-after-changing-the-mappings
## # GET _sql/_explain?sql=select _index, silk_script_release.keyword FROM filebeat-silk-log-* WHERE tags = '_sumary_report' GROUP BY _index, silk_script_release.keyword ORDER BY _index
## # GET _sql/_explain?sql=select silk_script.keyword, silk_transaction.keyword, silk_script_segment.keyword, silk_script_release.keyword, silk_transaction_status.keyword,  AVG(silk_transaction_time) as result_time, MIN(@timestamp_start) as timestamp_start FROM filebeat-silk-log-* WHERE  silk_transaction_status.keyword = 'Trans. ok[s]' OR silk_transaction_status.keyword = 'Trans. failed[s]' GROUP BY  silk_script.keyword, silk_transaction.keyword, silk_script_segment.keyword, silk_script_release.keyword, silk_transaction_status.keyword

## # autor: Leandro Sampaio Silva
## # Objetivo: Upload dos percentuais a serem considerados durante a analise comparativa dos releases do Silk

################################################################################################################
from elasticsearch import Elasticsearch, helpers

es = Elasticsearch([{'host': 'elk_one'}])

search_1 = es.search(index="filebeat-silk-log-*", body= {
  "from" : 0,
  "size" : 0,
  "_source" : {
    "includes" : [
      "_index",  
      "silk_script_release.keyword"
    ],
    "excludes" : [ ]
  },
  "stored_fields" : [
    "_index",
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
})

search_2 = es.search(index="filebeat-silk-log-*", body= {
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
              }
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
      "silk_script.keyword",
      "silk_transaction.keyword",
      "silk_script_segment.keyword",
      "silk_script_release.keyword",
      "silk_transaction_status.keyword",
      "AVG",
      "MIN"
  ],
  "stored_fields" : [
    "silk_script.keyword",
    "silk_transaction.keyword",
    "silk_script_segment.keyword",
    "silk_script_release.keyword",
    "silk_transaction_status.keyword"
  ],
  "aggregations" : {
    "silk_script.keyword" : {
      "terms" : {
        "field" : "silk_script.keyword",
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
        "silk_transaction.keyword" : {
          "terms" : {
            "field" : "silk_transaction.keyword",
            "size" : 10,
            "min_doc_count" : 1,
            "shard_min_doc_count" : 0,
            "show_term_doc_count_error" : False,
          },
          "aggregations" : {
            "silk_script_segment.keyword" : {
              "terms" : {
                "field" : "silk_script_segment.keyword",
                "size" : 10,
                "min_doc_count" : 1,
                "shard_min_doc_count" : 0,
                "show_term_doc_count_error" : False,
              },
              "aggregations" : {
                "silk_script_release.keyword" : {
                  "terms" : {
                    "field" : "silk_script_release.keyword",
                    "size" : 10,
                    "min_doc_count" : 1,
                    "shard_min_doc_count" : 0,
                    "show_term_doc_count_error" : False,
                  },
                  "aggregations" : {
                    "silk_transaction_status.keyword" : {
                      "terms" : {
                        "field" : "silk_transaction_status.keyword",
                        "size" : 10,
                        "min_doc_count" : 1,
                        "shard_min_doc_count" : 0,
                        "show_term_doc_count_error" : False,
                      },
                      "aggregations" : {
                        "result_time" : {
                          "avg" : {
                            "field" : "silk_transaction_time"
                          }
                        },
                        "timestamp_start" : {
                          "min" : {
                            "field" : "@timestamp_start"
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

update_data = []

from pytz import utc
from datetime import date, datetime



IDX = 'python-silk-'; SUFIX = 'percent-';

idx_pst = list()
idx_unq = set()

for index_loop in search_1['aggregations']['_index']['buckets']:

    for release_loop in index_loop['silk_script_release.keyword']['buckets']:

        if (release_loop['key'] not in idx_unq) :
            idx_unq.add(release_loop['key'])
            idx_pst.append(release_loop['key'])

while len(idx_pst) > 0 :

    for script_loop in search_2['aggregations']['silk_script.keyword']['buckets']:

        for transaction_loop in script_loop['silk_transaction.keyword']['buckets']:

            for segment_loop in transaction_loop['silk_script_segment.keyword']['buckets']:

                if any(idx_pst[0] == dic['key'] for dic in segment_loop['silk_script_release.keyword']['buckets']):

                    idx_new = {}
                    idx_new['_index'] =  IDX + SUFIX + date.today().strftime("%Y-%d-%m")
                    idx_new['_type'] = 'py_custom';
                    idx_new['_doc_type'] = 'py_custom';
                    idx_new['_source'] = {};

                    for release_loop in segment_loop['silk_script_release.keyword']['buckets']:

                        result_ok_time = 0
                        result_nok_count = 0
                        result_ok_count = 0

                        timestamp_start = ''

                        for status_loop in release_loop['silk_transaction_status.keyword']['buckets']:

                            timestamp_start = status_loop['timestamp_start']['value_as_string']

                            if status_loop['key'] == 'Trans. ok[s]' :

                                result_ok_time = status_loop['result_time']['value']
                                result_ok_count = status_loop['doc_count']

                            elif status_loop['key'] == 'Trans. failed[s]' :

                                result_nok_count = status_loop['doc_count']

                        if (idx_pst[0] == release_loop['key']) :

                            idx_new['_id'] = script_loop['key']+release_loop['key']+transaction_loop['key']+segment_loop['key']

                            idx_new['_source']['silk_script'] = script_loop['key']
                            idx_new['_source']['silk_script_release'] = release_loop['key']
                            idx_new['_source']['silk_transaction'] = transaction_loop['key']
                            idx_new['_source']['silk_script_segment'] = segment_loop['key']

                            idx_new['_source']['time_ok'] = result_ok_time
                            idx_new['_source']['count_nok'] = result_nok_count
                            idx_new['_source']['count_ok'] = result_ok_count

                            idx_new['_source']['@timestamp_start'] = timestamp_start

                            idx_new['_source']['percent_ref1'] = 0
                            idx_new['_source']['percent_ref2'] = 0
                            idx_new['_source']['percent_ref3'] = 0


                        if (len(idx_pst) > 1 and idx_pst[1] == release_loop['key']) :
                            idx_new['_source']['value_ref1'] = result_ok_time
                        elif 'value_ref1' not in idx_new['_source'] :
                            idx_new['_source']['value_ref1'] = 0

                        if (len(idx_pst) > 2 and idx_pst[2] == release_loop['key']) :
                            idx_new['_source']['value_ref2'] = result_ok_time
                        elif 'value_ref2' not in idx_new['_source'] :
                            idx_new['_source']['value_ref2'] = 0

                        if (len(idx_pst) > 3 and idx_pst[3] == release_loop['key']) :
                            idx_new['_source']['value_ref3'] = result_ok_time
                        elif 'value_ref3' not in idx_new['_source'] :
                            idx_new['_source']['value_ref3'] = 0

                    from decimal import Decimal
                    if (idx_new['_source']['time_ok']) :

                        if (idx_new['_source']['value_ref1'] ) :
                            idx_new['_source']['percent_ref1'] = float(round(Decimal(((idx_new['_source']['value_ref1']/idx_new['_source']['time_ok']) -1)*100),2))
                        elif (idx_new['_source']['value_ref1'] == 0) :
                            idx_new['_source']['percent_ref1'] = float(0)

                        if (idx_new['_source']['value_ref2']) :
                            idx_new['_source']['percent_ref2'] = float(round(Decimal(((idx_new['_source']['value_ref2']/idx_new['_source']['time_ok']) -1)*100),2))
                        elif (idx_new['_source']['value_ref2'] == 0) :
                            idx_new['_source']['percent_ref2'] = float(0)

                        if (idx_new['_source']['value_ref3']) :
                            idx_new['_source']['percent_ref3'] = float(round(Decimal(((idx_new['_source']['value_ref3']/idx_new['_source']['time_ok']) -1)*100),2))
                        elif (idx_new['_source']['value_ref3'] == 0) :
                            idx_new['_source']['percent_ref3'] = float(0)

                    else :

                        idx_new['_source']['percent_ref1'] = float(0)
                        idx_new['_source']['percent_ref2'] = float(0)
                        idx_new['_source']['percent_ref3'] = float(0)
                    print (idx_new)
                    update_data.append(idx_new)
    idx_pst.pop(0)

helpers.bulk(es,update_data)
es.indices.refresh(index= IDX + SUFIX + date.today().strftime("%Y-%d-%m"))
