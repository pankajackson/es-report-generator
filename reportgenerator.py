from elasticsearch import Elasticsearch
import pandas as pd
from urllib.parse import urlparse
from datetime import datetime
import os
import time


es_hosts =  'https://es.evega.co.in'
es_user = 'elastic'
es_password = '8PcTFW5TUL1VqQ11361H2A1u'
include_system_indices = False
report_dir_path = './reports'
report_file_name = '{cluster}-{dt}.csv'.format(cluster=urlparse(es_hosts).netloc,dt=datetime.now().strftime('%Y-%m-%d-%H-%M'))
output_path = os.path.join(report_dir_path, report_file_name)
data_buffer_size = 100
data_buffer_interval = 0.5 # value in sec

if not os.path.exists(report_dir_path):
    os.makedirs(report_dir_path)

def get_es_connection():
    es = Elasticsearch(
        hosts=es_hosts,
        http_auth=(es_user, es_password)
    )
    return es

def calc_size_unit(size=0, unit='B'):
    if size == 0:
        return {'size': 0, 'unit': 'B'}
    print('size in B: {siz}'.format(siz=size))
    size = ((size/1024)/1024)/1024
    unit = 'GB'
    print('size in G: {siz}'.format(siz=size))
    return '{size}{unit}'.format(size=round(size, 2),unit=unit)


def get_raw_indices(es=get_es_connection(), index="*"):
    es_res = es.indices.stats(index=index)
    return es_res

def write_to_csv(indices_data_list):
    df = pd.DataFrame(indices_data_list)
    df.to_csv(output_path, mode='a', index=False, header=not os.path.exists(output_path))


def parse_raw_dara(raw_indices=get_raw_indices()):
    shards_status = {
        "shards_total": raw_indices['_shards']['total'],
        "shards_successful": raw_indices['_shards']['successful'],
        "shards_failed": raw_indices['_shards']['failed'],
    }

    indices_data_list = []
    for indices in raw_indices['indices']:
        if (not str(indices).startswith('.')) or (include_system_indices and str(indices).startswith('.')):
            indices_data = {
                "indices": indices,
                "shard_primary_count": raw_indices['indices'][indices]['primaries']['shard_stats']['total_count'],
                "shard_replica_count": raw_indices['indices'][indices]['total']['shard_stats']['total_count'] - raw_indices['indices'][indices]['primaries']['shard_stats']['total_count'],
                "shard_total_count": raw_indices['indices'][indices]['total']['shard_stats']['total_count'],
                "docs_primary_count": raw_indices['indices'][indices]['primaries']['docs']['count'],
                "docs_replica_count": raw_indices['indices'][indices]['total']['docs']['count'] - raw_indices['indices'][indices]['primaries']['docs']['count'],
                "docs_total_count": raw_indices['indices'][indices]['total']['docs']['count'],
                "store_primary_size": calc_size_unit(raw_indices['indices'][indices]['primaries']['store']['size_in_bytes']),
                "store_replica_size": calc_size_unit(raw_indices['indices'][indices]['total']['store']['size_in_bytes'] - raw_indices['indices'][indices]['primaries']['store']['size_in_bytes']),
                "store_total_size": calc_size_unit(raw_indices['indices'][indices]['total']['store']['size_in_bytes'])
            }
            indices_data_list.append(indices_data)
            if len(indices_data_list) >= data_buffer_size:
                write_to_csv(indices_data_list)
                indices_data_list.clear()
                time.sleep(data_buffer_interval)
    write_to_csv(indices_data_list)




parse_raw_dara()