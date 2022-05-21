#!/usr/bin/env python3
from elasticsearch import Elasticsearch
import pandas as pd
from urllib.parse import urlparse
import argparse
from datetime import datetime
import pwd
import os
from getpass import getpass
import time
VERSION = 0.1


def get_es_connection(es_hosts, es_user=None, es_password=None, es_port=None, es_scheme=None, skip_cert=False):
    if es_user and es_password:
        es = Elasticsearch(
            hosts=es_hosts,
            port=es_port,
            scheme=es_scheme,
            verify_certs=not skip_cert,
            http_auth=(es_user, es_password)
        )
    else:
        es = Elasticsearch(
            hosts=es_hosts,
            port=es_port,
            scheme=es_scheme,
            verify_certs=not skip_cert
        )
    return es

def bytes_to_gb_converter(size=0):
    if size == 0:
        return 0
    size = ((size/1024)/1024)/1024
    return round(size, 3)


def get_raw_indices(es, index="*"):
    es_res = es.indices.stats(index=index)
    return es_res

def write_to_csv(indices_data_list, output_path):
    df = pd.DataFrame(indices_data_list)
    df.to_csv(output_path, mode='a', index=False, header=not os.path.exists(output_path))


def parse_raw_indices(raw_indices, include_system_indices=True, data_buffer_size=100, data_buffer_interval=0.5, output_path=os.path.join(pwd.getpwuid(os.getuid()).pw_dir, 'es-report-{dt}.csv'.format(dt=datetime.now().strftime('%Y-%m-%d-%H-%M')))):
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
                "store_primary_size(GB)": bytes_to_gb_converter(raw_indices['indices'][indices]['primaries']['store']['size_in_bytes']),
                "store_replica_size(GB)": bytes_to_gb_converter(raw_indices['indices'][indices]['total']['store']['size_in_bytes'] - raw_indices['indices'][indices]['primaries']['store']['size_in_bytes']),
                "store_total_size(GB)": bytes_to_gb_converter(raw_indices['indices'][indices]['total']['store']['size_in_bytes'])
            }
            indices_data_list.append(indices_data)
            if len(indices_data_list) >= data_buffer_size:
                write_to_csv(indices_data_list, output_path)
                indices_data_list.clear()
                time.sleep(data_buffer_interval)
    write_to_csv(indices_data_list, output_path)
    print('Report: {rp}'.format(rp=os.path.abspath(output_path)))
    print('Total Shards: {ts}'.format(ts=shards_status['shards_total']))
    print('Successful Shards: {ss}'.format(ss=shards_status['shards_successful']))
    print('Failed Shards: {fs}'.format(fs=shards_status['shards_failed']))


def _get_parser():
    parser = argparse.ArgumentParser(
        prog="esreportgenerator",
        epilog="Please report bugs at pankajackson@live.co.uk",
    )

    parser.add_argument(
        "endpoint",
        type=str,
        nargs='?',
        help="ES endpoint (eg. https://my-es-cluster.com:9200)",
    )

    parser.add_argument(
        "-P",
        "--port",
        required=False,
        type=str,
        help="ES Port (eg. 9200)",
    )

    parser.add_argument(
        "-u",
        "--username",
        required=False,
        type=str,
        help="ES username (eg. elastic)",
    )

    parser.add_argument(
        "-p",
        "--password",
        required=False,
        type=str,
        help="ES password (eg. secret)",
    )

    parser.add_argument(
        "-s",
        "--scheme",
        required=False,
        type=str,
        choices=['http', 'https'],
        help="ES password (eg. http or https)",
    )

    parser.add_argument(
        "--skip-cert",
        required=False,
        action="store_true",
        default=False,
        help="Skip ES Certificate verification",
    )

    parser.add_argument(
        "--skip-system-indices",
        required=False,
        action="store_true",
        default=False,
        help="Skip system indices in report (e.g. .kibana, .ml, .monitoring etc)",
    )

    parser.add_argument(
        "-o",
        "--output-dir",
        required=False,
        type=str,
        default=os.path.join(os.path.join(pwd.getpwuid(os.getuid()).pw_dir, 'Documents'), 'reports'),
        help="Base directory of report (eg. /home/james/Documents/report)",
    )

    parser.add_argument(
        "--buffer-size",
        required=False,
        type=int,
        default=100,
        help="Number of records process at a time (hint: Large buffer required more physical memory)",
    )

    parser.add_argument(
        "--buffer-interval",
        required=False,
        type=float,
        default=0.5,
        help="Wait time between two data buffer process (hint: Small interval required more CPU Resource)",
    )

    parser.add_argument(
        "-v", "--version", required=False, action="store_true", help="Show version"
    )

    return parser


def main():
    parser = _get_parser()
    args = parser.parse_args()

    if args.version:
        print(f"esreportgenerator: {VERSION}")
    else:
        es_user = None
        es_password = None
        es_port = None
        es_scheme = None
        skip_cert = False
        skip_system_indices = False

        es_hosts = args.endpoint
        if args.port:
            es_port = args.port
        if args.username:
            es_user = args.username
        if args.password:
            es_password = args.password
        if args.scheme:
            es_scheme = args.scheme
        if args.skip_cert:
            skip_cert = args.skip_cert
        if args.skip_system_indices:
            skip_system_indices = args.skip_system_indices
        report_dir_path = args.output_dir
        data_buffer_size = args.buffer_size
        data_buffer_interval = args.buffer_interval

        if es_user and not es_password:
            es_password = getpass("Please enter password for user \"{es_user}\": ".format(es_user=es_user))

        es = get_es_connection(
            es_hosts=es_hosts,
            es_user=es_user,
            es_password=es_password,
            es_port=es_port,
            es_scheme=es_scheme,
            skip_cert=skip_cert
        )
        if not es.ping():
            raise ValueError("Connection Failed")
        raw_indices = get_raw_indices(es=es)
        if not os.path.exists(report_dir_path):
            os.makedirs(report_dir_path)
        if not urlparse(es_hosts).netloc:
            report_file_name_prefix = es_hosts
        else:
            report_file_name_prefix = urlparse(es_hosts).netloc
        report_file_name = '{cluster}-{dt}.csv'.format(cluster=report_file_name_prefix,dt=datetime.now().strftime('%Y-%m-%d-%H-%M-%S'))
        output_path = os.path.join(report_dir_path, report_file_name)
        parse_raw_indices(
            raw_indices=raw_indices,
            include_system_indices=not skip_system_indices,
            data_buffer_size=data_buffer_size,
            data_buffer_interval=data_buffer_interval,
            output_path=output_path
        )


if __name__ == "__main__":
    main()