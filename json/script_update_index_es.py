#!/usr/bin/python
###########################################################################
import copy
import io
import json
import os
import re
from datetime import datetime
from urllib.request import build_opener, HTTPBasicAuthHandler
from elasticsearch import Elasticsearch, helpers
ES = Elasticsearch([{'host': 'elk_one'}])

if ES.indices.exists('test-index'):
    ES.indices.delete(index='test-index')


settings = {
    "settings": {
        "number_of_shards": 3,
        "number_of_replicas": 0,
        "index.mapper.dynamic":False
    },
    "mappings": {
      "stats_compare": {
        "properties": {
          "@timestamp": {
            "type": "date"
          },
          "after": {
            "type": "float"
          },
          "after_high": {
            "type": "float"
          },
          "after_low": {
            "type": "float"
          },
          "before": {
            "type": "float"
          },
          "before_high": {
            "type": "float"
          },
          "before_low": {
            "type": "float"
          },
          "calcular_percentis": {
            "type": "float"
          },
          "change_absolute": {
            "type": "float"
          },
          "change_normalized": {
            "type": "float"
          },
          "change_relevant": {
            "type": "boolean"
          },
          "change_significance": {
            "type": "boolean"
          },
          "comparar_percentis": {
            "type": "float"
          },
          "componente_externalid": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "componente_id": {
            "type": "long"
          },
          "componente_nome": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "componente_origem": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "componentes": {
            "type": "long"
          },
          "componentes_desvio": {
            "type": "long"
          },
          "config_id": {
            "type": "long"
          },
          "config_nome": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "config_nome_interno": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "criacao": {
            "type": "date"
          },
          "desvio": {
            "type": "boolean"
          },
          "desvio_absoluto_necessario": {
            "type": "float"
          },
          "desvio_mwu_necessario": {
            "type": "float"
          },
          "desvio_relativo_necessario": {
            "type": "float"
          },
          "dispersion_changed": {
            "type": "boolean"
          },
          "dispersion_p_value": {
            "type": "float"
          },
          "dispersion_statistic": {
            "type": "float"
          },
          "id": {
            "type": "long"
          },
          "location_changed": {
            "type": "boolean"
          },
          "location_p_value": {
            "type": "float"
          },
          "location_statistic": {
            "type": "float"
          },
          "metrica_externalid": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "metrica_nome": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "metrica_unidade": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "metricas": {
            "type": "long"
          },
          "metricas_desvio": {
            "type": "long"
          },
          "metricas_ignoradas": {
            "type": "long"
          },
          "mwu_significant": {
            "type": "boolean"
          },
          "percentile": {
            "type": "long"
          },
          "resource_uri": {
            "type": "text",
            "fields": {
              "keyword": {
                "type": "keyword",
                "ignore_above": 256
              }
            }
          },
          "snapshot_anterior": {
            "properties": {
              "componentes": {
                "type": "long"
              },
              "data": {
                "properties": {
                  "componente": {
                    "properties": {
                      "externalid": {
                        "type": "text",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      },
                      "id": {
                        "type": "long"
                      },
                      "nome": {
                        "type": "text",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      },
                      "origem": {
                        "type": "text",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      }
                    }
                  },
                  "resource_uri": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "snapshot_id": {
                    "type": "long"
                  },
                  "status": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  }
                }
              },
              "fim": {
                "type": "date"
              },
              "id": {
                "type": "long"
              },
              "inicio": {
                "type": "date"
              },
              "is_baseline": {
                "type": "boolean"
              },
              "metricas": {
                "type": "long"
              },
              "nome": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "pronto": {
                "type": "long"
              },
              "resource_uri": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "sistema_id": {
                "type": "long"
              },
              "sistema_nome": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "tamanho": {
                "type": "long"
              }
            }
          },
          "snapshot_atual": {
            "properties": {
              "componentes": {
                "type": "long"
              },
              "data": {
                "properties": {
                  "componente": {
                    "properties": {
                      "externalid": {
                        "type": "text",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      },
                      "id": {
                        "type": "long"
                      },
                      "nome": {
                        "type": "text",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      },
                      "origem": {
                        "type": "text",
                        "fields": {
                          "keyword": {
                            "type": "keyword",
                            "ignore_above": 256
                          }
                        }
                      }
                    }
                  },
                  "resource_uri": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  },
                  "snapshot_id": {
                    "type": "long"
                  },
                  "status": {
                    "type": "text",
                    "fields": {
                      "keyword": {
                        "type": "keyword",
                        "ignore_above": 256
                      }
                    }
                  }
                }
              },
              "fim": {
                "type": "date"
              },
              "id": {
                "type": "long"
              },
              "inicio": {
                "type": "date"
              },
              "is_baseline": {
                "type": "boolean"
              },
              "metricas": {
                "type": "long"
              },
              "nome": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "pronto": {
                "type": "long"
              },
              "resource_uri": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "sistema_id": {
                "type": "long"
              },
              "sistema_nome": {
                "type": "text",
                "fields": {
                  "keyword": {
                    "type": "keyword",
                    "ignore_above": 256
                  }
                }
              },
              "tamanho": {
                "type": "long"
              }
            }
          }
        }
      }
    }
}

ES.indices.create(index='test-index',  body=settings)


#root_url = 'http://ec2-54-207-203-38.sa-east-1.compute.amazonaws.com/capacidade'
root_url = 'http://192.168.56.190/capacidade'
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
  json_loaded = json.load(f)

for summary_compare in json_loaded['objects']:

  id_snap_anterior = summary_compare['snapshot_anterior']['id']
  id_snap_atual    = summary_compare['snapshot_atual']['id']

  summary_compare['@timestamp'] = summary_compare['criacao']

  update_data = list()

  with http.open('{}/comparativo/{}/result/'.format(api_url, summary_compare['id'])) as u, io.TextIOWrapper(u) as f:
    json_loaded = json.load(f)

  for kmetric, vmetric  in json_loaded.items():
    if not re.match('.*success @ \d+\)', vmetric['metrica_nome']):
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
          
      count=0
      while count <100:
        if count not in vmetric["calcular_percentis"]:    
          cp_metric = copy.deepcopy(vmetric)
          cp_metric["percentile"] = count
          
          cp_metric["before"] = data1[int(len_1/100*count)][1]
          cp_metric["after" ] = data2[int(len_2/100*count)][1]
          cp_metric.update(summary_compare)

          delta_date = data2[0][0] - data1[0][0]

          cp_metric["before_date"] = datetime.fromtimestamp(int(delta_date + data1[int(len_1/100*count)][0])/1000)
          cp_metric["after_date"]  = datetime.fromtimestamp(int(             data2[int(len_2/100*count)][0])/1000)

          del cp_metric["stats"]
          update_data.append(cp_metric)
        count+=1

      for kstat, vstat in vmetric["stats"].items():
        if re.match('p\d+',kstat):
      
          vstat["percentile"] = int(kstat[1:])
        
        vstat.update(summary_compare)
        cp_metric = copy.deepcopy(vmetric)
        vstat.update(cp_metric)
        
        del vstat["stats"]
        update_data.append(vstat)

for result in update_data:
  #print(result)
  ES.index(index="test-index", doc_type="stats_compare", body=result)