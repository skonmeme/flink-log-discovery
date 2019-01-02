#!/usr/bin/env python

import argparse
import errno
import json
import logging
import os
import re
import requests
import sys
import time
from functools import partial

logger = logging.getLogger("discovery")


def get_yarn_application_info(app_id, rm_addr):
    r = requests.get(rm_addr + '/ws/v1/cluster/apps/' + app_id)
    if r.status_code != 200:
        return {}

    decoded = r.json()
    return decoded['app'] if 'app' in decoded else {}


def get_flink_cluster_overview(jm_url):
    r = requests.get(jm_url+'/overview')
    if r.status_code != 200:
        return {}
    decoded = r.json()
    return decoded


def get_taskmanager_ids(jm_url):
    r = requests.get(jm_url + '/taskmanagers')
    if r.status_code != 200:
        return []

    decoded = r.json()
    if 'taskmanagers' not in decoded:
        return []

    return [tm['id'] for tm in decoded['taskmanagers']]


def find_flink_log_addresses(app_id, rm_addr):
    count = 1
    logs = {}

    while True:
        if count > 10:
            return {}

        app_info = get_yarn_application_info(app_id, rm_addr)
        logger.debug("Application ID: {}\nApplication Info: {}".format(app_id, app_info))
        if 'trackingUrl' not in app_info:
            logger.info("Application ID: {} does not have tackingUrl".format(app_id, count))
            count += 1
            time.sleep(1)
            continue

        jm_url = app_info['trackingUrl']
        jm_url = jm_url[:-1] if jm_url.endswith('/') else jm_url
        logs['jm_url'] = {'url': jm_url}

        overview = get_flink_cluster_overview(jm_url)
        version = overview['flink-version']
        taskmanagers = overview['taskmanagers']
        logger.debug("Flink overview: {}\n  Version: {}\n  Task managers: {}".format(overview, version, taskmanagers))

        logs['jm_log'] = {'url': jm_url + "/jobmanager/log"}

        if app_info['runningContainers'] == 1:
            logger.info("runningContainers(%d) is 1" % (app_info['runningContainers'],))
            count += 1
            time.sleep(1)
            continue

        if app_info['runningContainers'] != taskmanagers + 1:
            logger.info("runningContainers(%d) != jobmanager(1)+taskmanagers(%d)" % (app_info['runningContainers'], taskmanagers))
            count += 1
            time.sleep(1)
            continue

        tm_ids = get_taskmanager_ids(jm_url)
        logs['tm_logs'] = [ {'id': tm_id, 'url': jm_url + "/taskmanagers/" + tm_id + "/log"} for tm_id in tm_ids ]
        break
    logger.debug(logs)

    return logs


def generate_logstash_conf(logs, target_path, es_addr):
    # intput
    logstash_conf =          "{\n" \
                             "  input {\n"
    for log in logs:
        logstash_conf +=     "    http_poller {\n" \
                             "      type => \"{}\"".format(log['app_id']) + "\n" \
                             "      urls => {\n" \
                             "        url => {}".format(log['jm_log']['url']) + "\n" \
                             "      }\n" \
                             "    }\n"
        for tm_log in log['tm_log']:
            logstash_conf += "    http_poller {\n" \
                             "      type => \"{}-{}\"".format(log['app_id'], tm_log['id']) + "\n" \
                             "      urls => {\n" \
                             "        url => {}".format(tm_log['url']) + "\n" \
                             "      }\n" \
                             "    }\n"
    logstash_conf +=         "  }\n"
    # filter
    # output
    logstash_conf +=         "  output {\n"
    for log in logs:
        logstash_conf +=     "    if [type] == {} {".format(log['app_id']) + "\n" \
                             "      elasticsearch {\n" \
                             "        host => {}".format(es_addr) + "\n" \
                             "        index => flink-{}".format(log['app_id']) + "\n" \
                             "      }\n" \
                             "    }\n"
        for tm_log in log['tm_logs']:
            logstash_conf += "    if [type] == \"{}-{}\" {".format(log['app_id'], tm_log['id']) + "\n" \
                             "      elasticsearch {\n" \
                             "        host => {}".format(es_addr) + "\n" \
                             "        index => flink-{}-{}".format(log['app_id'], tm_log['id']) + "\n" \
                             "      }\n" \
                             "    }\n"
    logstash_conf +=         "  }\n" \
                             "}"

    if target_path is not None:
        with open(target_path, 'w') as file:
            file.write(logstash_conf)
    else:
        print(logstash_conf)

    return logstash_conf


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Discover Flink clusters on Hadoop YARN for Prometheus')
    parser.add_argument('rm_addr', type=str,
                        help='(required) Specify yarn.resourcemanager.webapp.address of your YARN cluster.')
    parser.add_argument('es_addr', type=str,
                        help='(required) Specify ElasticSearch address.')
    parser.add_argument('--app-id', type=str,
                        help='If specified, this program runs once for the application. '
                             'Otherwise, it runs as a service.')
    parser.add_argument('--name-filter', type=str,
                        help='A regex to specify applications to watch.')
    parser.add_argument('--target-path', type=str,
                        help='If specified, this program writes the target information to a file on the directory. '
                             'Files are named after the application ids. '
                             'Otherwise, it prints to stdout.')
    parser.add_argument('--poll-interval', type=int, default=15,
                        help='Polling interval to YARN in seconds '
                             'to check applications that are newly added or recently finished. '
                             'Default is 5 seconds.')
    parser.add_argument('-d', action="store_true",
                        help='Display debugging messages (with -v).')
    parser.add_argument('-v', action="store_true",
                        help='Display verbose messages.')

    args = parser.parse_args()

    if args.d:
        args.v = True
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    if args.v:
        logger.addHandler(logging.StreamHandler(stream=sys.stderr))

    app_id = args.app_id
    name_filter_regex = None if args.name_filter is None else re.compile(args.name_filter)
    rm_addr = args.rm_addr if "://" in args.rm_addr else "http://" + args.rm_addr
    rm_addr = rm_addr[:-1] if rm_addr.endswith('/') else rm_addr
    es_addr = args.es_addr
    target_path = args.target_path

    if target_path is not None and not os.path.isdir(target_path):
        logger.error('cannot find', target_path)
        sys.exit(1)

    if app_id is not None:
        log_url = {app_id: find_flink_log_addresses(app_id, rm_addr)}
        if log_url[app_id] != {}:
            generate_logstash_conf(log_url, target_path, es_addr)
    else:
        logger.info("start polling every " + str(args.poll_interval) + " seconds.")
        running_prev = {}
        while True:
            running_cur = {}
            added = set()
            removed = set()

            r = requests.get(rm_addr+'/ws/v1/cluster/apps')
            if r.status_code != 200:
                logger.error("Failed to connect to the server."
                             "The status code is " + r.status_code)
                break

            decoded = r.json()
            apps = decoded['apps']['app']
            if name_filter_regex is not None:
                apps = list(filter(lambda app: name_filter_regex.match(app['name']), apps))
            for app in apps:
                if app['state'].lower() == 'running':
                    running_cur[app['id']] = app

            if running_prev != running_cur:
                added = set(running_cur.keys()) - set(running_prev.keys())
                removed = set(running_prev.keys()) - set(running_cur.keys())

            if len(added) + len(removed) > 0:
                logger.info('==== {} ===='.format(time.strftime("%c")))
                logger.info("{} running apps : ".format(len(running_cur)))
                logger.info("{} added        : ".format(added))
                logger.info("{} removed      : ".format(removed))

                logs = { app_id: find_flink_log_addresses(app_id, rm_addr) for app_id in running_cur.keys() }
                [ logs.pop(key, None) for (key, value) in logs.items() if value == {} ]
                if len(logs) > 0:
                    generate_logstash_conf(logs, target_path, es_addr)

            running_prev = running_cur
            time.sleep(args.poll_interval)