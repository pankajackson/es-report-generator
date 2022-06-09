#!/usr/bin/env python3
from elasticsearch import Elasticsearch
from urllib.parse import urlparse
from datetime import datetime
from getpass import getpass
from pygrok import Grok as gk
import yaml
from pathlib import Path
import fnmatch
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn as sns
import uuid
import base64
import argparse
import pwd
import os
import pathlib
import time
import warnings
VERSION = 1.02


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

def _to_gb_converter(size=0, unit='B'.upper()):
    size = float(size)
    if size == 0:
        return 0
    if str(unit).upper() == 'B':
        size = ((size/1024)/1024)/1024
    elif str(unit).upper() == 'KB':
        size = (size/1024)/1024
    elif str(unit).upper() == 'MB':
        size = size/1024
    elif str(unit).upper() == 'GB':
        size = size
    elif str(unit).upper() == 'TB':
        size = size*1024
    return round(size, 3)

def parse_config(config_path):
    conf = yaml.safe_load(Path(config_path).read_text())
    return conf

def get_owner(config, index_pattern):
    try:
        for owner in config['owners']:
            for proj in owner["projects"]:
                for pattern in proj['index_patterns']:
                    if pattern:
                        if fnmatch.fnmatch(index_pattern, pattern) or fnmatch.fnmatch(index_pattern, 'shrink-{p}'.format(p=str(pattern))):
                            return {'owner': owner["name"], 'project': proj['name']}
        return {'owner': None, 'project': None}
    except Exception as e:
        print('ERROR: {error}'.format(error=e))
        return {'owner': None, 'project': None}


def get_raw_indices(es, index="*"):
    es_res = es.indices.stats(index=index, forbid_closed_indices=True)
    return es_res

def get_raw_indices_web(es, index="*"):
    es_res = es.cat.indices(index=index)
    return es_res

def write_to_csv(indices_data_list, output_path):
    df = pd.DataFrame(indices_data_list)
    df.to_csv(output_path, mode='a', index=False, header=not os.path.exists(output_path))


def parse_raw_indices(raw_indices, include_system_indices=True, data_buffer_size=100, data_buffer_interval=0.5, output_path=os.path.join(pwd.getpwuid(os.getuid()).pw_dir, 'es-report-{dt}.csv'.format(dt=datetime.now().strftime('%Y-%m-%d-%H-%M'))), config=None, ilm_policies=None, indices_ilm=None):
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
                "store_primary_size(GB)": _to_gb_converter(raw_indices['indices'][indices]['primaries']['store']['size_in_bytes']),
                "store_replica_size(GB)": _to_gb_converter(raw_indices['indices'][indices]['total']['store']['size_in_bytes'] - raw_indices['indices'][indices]['primaries']['store']['size_in_bytes']),
                "store_total_size(GB)": _to_gb_converter(raw_indices['indices'][indices]['total']['store']['size_in_bytes'])
            }
            if config:
                owner_details = get_owner(config=config, index_pattern=indices)
                indices_data['owner'] = owner_details['owner']
                indices_data['project'] = owner_details['project']
            if ilm_policies and indices_ilm:
                if indices in indices_ilm["indices"].keys():
                    if indices_ilm["indices"][indices]["managed"]:
                        policy = indices_ilm["indices"][indices]["policy"]
                        indices_data["phase"] = indices_ilm["indices"][indices]["phase"]
                        for phase in ilm_policies[policy]["policy"]["phases"].keys():
                            if phase == "hot":
                                for rollover in ilm_policies[policy]["policy"]["phases"][phase]["actions"]["rollover"].keys():
                                    indices_data["drp_" + phase + "_" + rollover] = ilm_policies[policy]["policy"]["phases"][phase]["actions"]["rollover"][rollover]
                            else:
                                indices_data["drp_" + phase + "_min_age"] = ilm_policies[policy]["policy"]["phases"][phase]["min_age"]
            indices_data_list.append(indices_data)
            if len(indices_data_list) >= data_buffer_size:
                write_to_csv(indices_data_list, output_path)
                indices_data_list.clear()
                time.sleep(data_buffer_interval)

    write_to_csv(indices_data_list, output_path)
    return os.path.abspath(output_path)

def parse_size(raw_size):
    pattern = '%{BASE10NUM:size}%{NOTSPACE:unit}'
    if raw_size:
        parsed_size = gk(pattern=pattern).match(raw_size)
        return parsed_size


def parse_raw_indices_web(raw_indices, include_system_indices=True, data_buffer_size=100, data_buffer_interval=0.5, output_path=os.path.join(pwd.getpwuid(os.getuid()).pw_dir, 'es-report-{dt}.csv'.format(dt=datetime.now().strftime('%Y-%m-%d-%H-%M'))), config=None, ilm_policies=None, indices_ilm=None):
    indices_data_list = []
    for indices in str(raw_indices).splitlines():
        try:
            if (not str(indices.split()[2]).startswith('.')) or (include_system_indices and str(indices.split()[2]).startswith('.')):
                indices_data = {}
                indices_data["indices"] = indices.split()[2]
                indices_data["shard_primary_count"] = indices.split()[4]
                indices_data["shard_replica_count"] = indices.split()[5]
                indices_data["shard_total_count"] = int(indices.split()[4]) + int(indices.split()[5])
                indices_data["docs_primary_count"] = indices.split()[6]
                indices_data["store_primary_size(GB)"] = _to_gb_converter(size=parse_size(raw_size=indices.split()[9])["size"], unit=parse_size(raw_size=indices.split()[9])["unit"])
                indices_data["store_total_size(GB)"] = _to_gb_converter(size=parse_size(raw_size=indices.split()[8])["size"], unit=parse_size(raw_size=indices.split()[8])["unit"])
                if config:
                    owner_details = get_owner(config=config, index_pattern=indices.split()[2])
                    indices_data['owner'] = owner_details['owner']
                    indices_data['project'] = owner_details['project']
                if ilm_policies and indices_ilm:
                    if indices.split()[2] in indices_ilm["indices"].keys():
                        if indices_ilm["indices"][indices.split()[2]]["managed"]:
                            policy = indices_ilm["indices"][indices.split()[2]]["policy"]
                            indices_data["phase"] = indices_ilm["indices"][indices.split()[2]]["phase"]
                            for phase in ilm_policies[policy]["policy"]["phases"].keys():
                                if phase == "hot":
                                    for rollover in ilm_policies[policy]["policy"]["phases"][phase]["actions"]["rollover"].keys():
                                        indices_data["drp_" + phase + "_" + rollover] = ilm_policies[policy]["policy"]["phases"][phase]["actions"]["rollover"][rollover]
                                else:
                                    indices_data["drp_" + phase + "_min_age"] = ilm_policies[policy]["policy"]["phases"][phase]["min_age"]
                indices_data_list.append(indices_data)
                if len(indices_data_list) >= data_buffer_size:
                    write_to_csv(indices_data_list, output_path)
                    indices_data_list.clear()
                    time.sleep(data_buffer_interval)
        except Exception as e:
            print('ERROR: {error}'.format(error=e))
            print('Skipping indices {indices}: indices is in {state} state and {health} health'.format(indices=indices.split()[2], state=indices.split()[1], health=indices.split()[0]))
            print(indices)


    write_to_csv(indices_data_list, output_path)
    return os.path.abspath(output_path)

def load_df(csv_path=None):
    if not csv_path:
        return None
    df = pd.read_csv(csv_path)
    df['owner'].fillna(value='UNKNOWN', inplace=True)
    df['project'].fillna(value='UNKNOWN', inplace=True)
    return df

def generate_graphs(df_csv_path=None, out_path=None):
    if not df_csv_path:
        return None
    df = load_df(csv_path=df_csv_path)
    df_size_per_owner  = df.groupby(['owner']).sum().reset_index()
    df_size_per_project = df.groupby(['project']).sum().reset_index()
    print(df_size_per_owner)
    print(df_size_per_project)

    # Set figure size
    sns.set(rc={'figure.figsize': (16, 16)})

    # Plot first Bar Chart
    plt.subplot(2, 1, 1)
    # Plot first x axis
    df_size_per_owner_bar_plt = sns.barplot(
        data=df_size_per_owner.sort_values(by=['store_total_size(GB)'], ascending=False),
        x='owner',
        y='store_total_size(GB)',
        dodge=False,
        alpha=0.5,
        linestyle='-',
        linewidth=2,
        edgecolor='k',
        estimator=np.max,
        ci=None,
        palette='hls',
        saturation=0.3,

    )
    # Set X labels
    for i in df_size_per_owner_bar_plt.containers:
        df_size_per_owner_bar_plt.bar_label(i, )
        df_size_per_owner_bar_plt.set(xlabel='Owner', ylabel='Storage Size (GB)')
    # Rotate X labels
    df_size_per_owner_bar_plt.set_xticklabels(df_size_per_owner_bar_plt.get_xticklabels(), rotation=25, ha="right")

    # Plot second Bar Chart
    plt.subplot(2, 1, 2)
    # Plot first x axis
    df_size_per_project_bar_plt = sns.barplot(
        data=df_size_per_project.sort_values(by=['store_total_size(GB)'], ascending=False),
        x='project',
        y='store_total_size(GB)',
        dodge=False,
        alpha=0.5,
        linestyle='-',
        linewidth=2,
        edgecolor='k',
        estimator=np.max,
        ci=None,
        palette='hls',
        saturation=0.3,

    )
    # Set Bars labels
    for i in df_size_per_project_bar_plt.containers:
        df_size_per_project_bar_plt.bar_label(i, )

    # Plot second x axis
    df_size_per_project_bar_plt = sns.barplot(
        data=df_size_per_project.sort_values(by=['store_total_size(GB)'], ascending=False),
        x='project',
        y='store_primary_size(GB)',
        dodge=False,
        alpha=0.5,
        linestyle='-',
        linewidth=2,
        edgecolor='k',
        estimator=np.max,
        ci=None,
        palette='hls',
        saturation=0.5,
    )
    # Set Bars labels
    df_size_per_project_bar_plt.set(xlabel='Project',ylabel='Storage Size (GB)')

    df_size_per_project_bar_plt.set_xticklabels(df_size_per_project_bar_plt.get_xticklabels(), rotation=25, ha="right")

    # Save Graph
    df_size_per_project_bar_plt.figure.savefig(os.path.join(out_path, 'barplot-project.png'), dpi=100)
    df_size_per_owner_bar_plt.figure.clear()
    return os.path.join(out_path, 'barplot.png')

def gen_visualization(df_csv_path=None, config=None, output_path=pwd.getpwuid(os.getuid()).pw_dir):
    if not config:
        return "INFO: Skipping Graph, Config unavailable"
    if not "graphs" in config.keys():
        return "INFO: Skipping Graph, Graph Config unavailable"
    if not df_csv_path:
        return "ERROR: Skipping Graph, CSV File not found"
    df = load_df(csv_path=df_csv_path)
    if df.shape == [0,0]:
        return "INFO: Skipping Graph, DataFrame unavailable"
    fig_length = 0
    final_graphs_list = []
    for idx, graph in enumerate(config['graphs']):
        if graph['type'] == 'barchart':
            try:
                if graph['properties']['x'] in df.columns and  graph['properties']['y1'] in df.columns:
                    length = df.groupby(graph['properties']['x']).sum().reset_index().shape[0]
                    if length > fig_length:
                        fig_length = length
                    if graph not in final_graphs_list:
                        final_graphs_list.append(graph)
                    else:
                        print('Skipping no. {index} graph \"{gf}\" as it already exist.'.format(index=idx + 1, gf=graph['name']))
                else:
                    print("Defined Header unavailable, Skipping graph \"{graph}\"".format(graph=graph['name']))
            except Exception as e:
                print("ERROR: {error}".format(error=e))

    if len(final_graphs_list) <= 0:
        return None
    fig_height = 8*(len(final_graphs_list) - len(final_graphs_list)/4)

    fig, axes = plt.subplots(len(final_graphs_list), 1, figsize=(fig_length, fig_height))
    fig.suptitle('Pokemon Stats by Generation')
    fig.subplots_adjust(hspace=0.3, wspace=0.3)
    for graph in final_graphs_list:
        df_aggr = df.groupby(graph['properties']['x']).sum().reset_index().sort_values(by=graph['properties']['y1'], ascending=False)

        sns.barplot(
            ax=axes[final_graphs_list.index(graph)],
            data=df_aggr,
            x=graph['properties']['x'],
            y=graph['properties']['y1'],
            dodge=False,
            alpha=0.5,
            linestyle='-',
            linewidth=2,
            edgecolor='k',
            estimator=np.max,
            ci=None,
            palette='hls',
            saturation=0.3,
        )

        for i in axes[final_graphs_list.index(graph)].containers:
            axes[final_graphs_list.index(graph)].bar_label(i, )

        if 'y2' in graph['properties'].keys() and graph['properties']['y2']:
            sns.barplot(
                ax=axes[final_graphs_list.index(graph)],
                data=df_aggr,
                x=graph['properties']['x'],
                y=graph['properties']['y2'],
                dodge=False,
                alpha=0.5,
                linestyle='-',
                linewidth=2,
                edgecolor='k',
                estimator=np.max,
                ci=None,
                palette='hls',
                saturation=0.3,
            )

        axes[final_graphs_list.index(graph)].set_title(graph['name'])

        if "xlabel" in graph['properties'].keys() and graph['properties']['xlabel']:
            axes[final_graphs_list.index(graph)].set(xlabel=graph['properties']['xlabel'])
        if "ylabel" in graph['properties'].keys() and graph['properties']['ylabel']:
            axes[final_graphs_list.index(graph)].set(ylabel=graph['properties']['ylabel'])
        axes[final_graphs_list.index(graph)].set_xticklabels(axes[final_graphs_list.index(graph)].get_xticklabels(), rotation=25, ha="right")

    plt.savefig(os.path.join(output_path, 'chart.png'), dpi=100)
    plt.clf()
    return os.path.join(output_path, 'chart.png')


def _get_parser():
    parser = argparse.ArgumentParser(
        prog="esreportgen",
        epilog="Please report bugs at pankajackson@live.co.uk",
    )

    parser.add_argument(
        "endpoint",
        type=str,
        nargs='?',
        help="ES endpoint (eg. https://my-es-cluster.com:9200)",
    )

    parser.add_argument(
        "-c",
        "--config",
        required=False,
        type=str,
        help="config file path containing owner details",
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
        "-P",
        "--port",
        required=False,
        type=str,
        help="ES Port (eg. 9200)",
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
        "--skip-config",
        required=False,
        action="store_true",
        default=False,
        help="Skip config file owner less details",
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
    warnings.filterwarnings('ignore')
    parser = _get_parser()
    args = parser.parse_args()

    if args.version:
        print("esreportgen: {VERSION}".format(VERSION=VERSION))
    else:
        es_user = None
        es_password = None
        es_port = None
        es_scheme = None
        skip_cert = False
        skip_system_indices = False
        skip_config = False
        config_path = "/etc/esreportgen.yaml"

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
        if args.skip_config:
            skip_config = args.skip_config
        if args.config:
            config_path = args.config
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

        if not os.path.exists(config_path) and not skip_config:
            raise ValueError('unable to read configuration file {c}, please use --skip-config to skip owner based details.'.format(c=config_path))
        elif skip_config:
            config = None
        else:
            try:
                config = parse_config(config_path)
            except Exception as e:
                print('ERROR: {error}'.format(error=e))
                raise ValueError('Unable to Understand config, please check file {file}'.format(file=config_path))
        try:
            es.cluster.health()
            try:
                ilm_policies = es.ilm.get_lifecycle()
                indices_ilm = es.ilm.explain_lifecycle(index='*')
            except Exception as e:
                print('ERROR: {error}'.format(error=e))
                print('Unable to Fetch ilm policies')
                ilm_policies = None
                indices_ilm = None
            try:
                raw_indices = get_raw_indices(es=es)
                if not os.path.exists(report_dir_path):
                    os.makedirs(report_dir_path)
                if not urlparse(es_hosts).netloc:
                    report_file_name_prefix = es_hosts
                else:
                    report_file_name_prefix = urlparse(es_hosts).netloc
                report_file_name_suffix = base64.b64encode(str(uuid.uuid4()).encode("ascii")).decode("ascii")[:6]
                report_file_name = '{cluster}-{dt}-{sf}'.format(cluster=report_file_name_prefix,
                                                                dt=datetime.now().strftime('%Y-%m-%d-%H-%M-%S'),
                                                                sf=report_file_name_suffix)
                output_path = os.path.join(report_dir_path, "{out_file_name}.csv".format(
                    out_file_name=str(report_file_name).replace('.', '-').replace(':', '-').replace('/', '-')))
                report_csv = parse_raw_indices(
                    raw_indices=raw_indices,
                    include_system_indices=not skip_system_indices,
                    data_buffer_size=data_buffer_size,
                    data_buffer_interval=data_buffer_interval,
                    output_path=output_path,
                    config=config,
                    ilm_policies=ilm_policies,
                    indices_ilm=indices_ilm
                )
            except Exception as e:
                print('ERROR: {error}'.format(error=e))
                print('Looks like ES version compatibility problem!')
                print('Trying with ES Web API')
                raw_indices = get_raw_indices_web(es=es)
                if not os.path.exists(report_dir_path):
                    os.makedirs(report_dir_path)
                if not urlparse(es_hosts).netloc:
                    report_file_name_prefix = es_hosts
                else:
                    report_file_name_prefix = urlparse(es_hosts).netloc
                report_file_name_suffix = base64.b64encode(str(uuid.uuid4()).encode("ascii")).decode("ascii")[:6]
                report_file_name = '{cluster}-{dt}-{sf}'.format(cluster=report_file_name_prefix,
                                                                dt=datetime.now().strftime('%Y-%m-%d-%H-%M-%S'),
                                                                sf=report_file_name_suffix)
                output_path = os.path.join(report_dir_path, "{out_file_name}.csv".format(
                    out_file_name=str(report_file_name).replace('.', '-').replace(':', '-').replace('/', '-')))
                report_csv = parse_raw_indices_web(
                    raw_indices=raw_indices,
                    include_system_indices=not skip_system_indices,
                    data_buffer_size=data_buffer_size,
                    data_buffer_interval=data_buffer_interval,
                    output_path=output_path,
                    config=config,
                    ilm_policies=ilm_policies,
                    indices_ilm=indices_ilm
                )
            if report_csv:
                print('Report: {rp}'.format(rp=report_csv))
                # graphs = generate_graphs(df_csv_path=report_csv, out_path=pathlib.Path(report_csv).parent.absolute())
                graphs = gen_visualization(df_csv_path=report_csv, config=config,
                                           output_path=pathlib.Path(report_csv).parent.absolute())
                print('Graph: {gf}'.format(gf=graphs))

        except Exception as e:
            print('ERROR: {error}'.format(error=str(e)))


if __name__ == "__main__":
    main()