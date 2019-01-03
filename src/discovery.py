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
    logs = {'app_id': app_id}

    while True:
        if count > 10:
            return None

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
        logs['tm_logs'] = { tm_id: {'url': jm_url + "/taskmanagers/" + tm_id + "/log"} for tm_id in tm_ids }
        break
    logger.debug(logs)

    return logs


def generate_url_output(logs):
    output = rm_addr + '\n'
    for log in logs:
        output += log['app_id'] + '\t'+ log['jm_log']['url'] + '\n'
        for (tm_id, tm_log) in log['tm_logs'].items():
            output += tm_id + '\t' + tm_log['url'] + '\n'
        output += '_\t_\n'

    return output


def keep_tracking_flink(rm_addr, options):
    logger.info("start polling every " + str(args.poll_interval) + " seconds.")

    running_prev = {}
    while True:
        running_cur = {}
        added = set()
        removed = set()
        r = requests.get(rm_addr + '/ws/v1/cluster/apps')
        if r.status_code != 200:
            logger.error("Failed to connect to the server."
                         "The status code is " + r.status_code)
            break
        decoded = r.json()
        apps = decoded['apps']['app']
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
            logs = filter(lambda x: x is not None,
                          [find_flink_log_addresses(app_id, rm_addr) for app_id in running_cur.keys()])
            if len(logs) > 0:
                url_output = generate_url_output(logs)
                if args.db_dir is not None:
                    with open(args.db_dir + '/logs.db', 'w') as file:
                        file.write(url_output)
                else:
                    print(url_output)
                logger.debug(url_output)
        running_prev = running_cur
        time.sleep(args.poll_interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Discover Flink clusters on Hadoop YARN for Prometheus')
    parser.add_argument('rm_addr', type=str,
                        help='(required) Specify yarn.resourcemanager.webapp.address of your YARN cluster.')
    parser.add_argument('--poll-interval', type=int, default=5,
                        help='Polling interval to YARN in seconds '
                             'to check applications that are newly added or recently finished. '
                             'Default is 60 seconds.')
    parser.add_argument('--db-dir', type=str,
                        help='If specified, this program keeps tracking log URLs in this directory. '
                             'If not specified, discovered log URLs are printed out.')
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

    rm_addr = args.rm_addr if "://" in args.rm_addr else "http://" + args.rm_addr
    rm_addr = rm_addr[:-1] if rm_addr.endswith('/') else rm_addr

    if args.db_dir is not None and not os.path.isdir(args.db_dir):
        logger.error('cannot find', args.db_dir)
        sys.exit(1)

    keep_tracking_flink(rm_addr, args)