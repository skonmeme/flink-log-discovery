import argparse
import errno
import json
import logging
import logstash
import os
import requests
import sys
import time

logger = logging.getLogger("logstash")
logstash_logger = logging.getLogger("flink")
logger.setLevel(logging.INFO)
l = logstash.TCPLogstashHandler("", "", version=1)


def set_logs_from_urls(urls, old_logs):
    logs = old_logs

    # remove old urls and set remain urls to removable
    logs = filter(lambda x: x['removable'] == False, logs)
    for (id, log) in logs.items():
        if log['removable']:
            logs.pop(id, None)
        else:
            log['removable'] = True

    # update current url information
    for (id, url) in urls.items():
        if id in logs.keys():
            # presnet url
            logs[id]['removable'] = False
        else:
            # new url
            logs[id] = url

    return logs


def get_log_messages(log):
    r = requests.get(log['url'])
    if r.status_code != 200:
        return None

    return r.text()


def push_log_to_logstash(options):
    logger.info("start logging every " + str(options.logging_interval) + " seconds.")

    # old _urls
    old_urls = None
    # old_logs
    logs_path = options.db_dir + '/logs.db'
    urls_path = options.db_dir + '/urls.db'
    if os.path.exists(logs_path):
        with open(logs_path, 'r') as file:
            old_logs = json.loads(file.read())
    else:
        old_logs = {}
    logger.debug("Initial log URL information : {}".format(old_logs))
    # count
    count = 0
    while True:
        if os.path.exists(urls_path):
            with open(urls_path, 'r') as file:
                urls = json.loads(file.read())
        else:
            count += 1
            if count >= 600:
                logger.error("No log URL information in 600s)")
                sys.exit(errno.ENOENT)
            time.sleep(1)
            continue

        if old_urls != urls:
            logs = set_logs_from_urls(urls, old_logs)
            logger.info("log URLs: {}".format(logs))
        for (id, log) in logs.items():
            messages = get_log_messages(log)
            position = log['position']
            for line in messages.splitlines()[position:]:
                message = {'log': line, 'app_id': log['app_id'], 'type': log['type'], 'id': log['id']}
                logstash_logger.info(json.dumps(message))
                log['position'] += 1

        if len(logs) > 0:
            with open(logs_path, 'w') as file:
                file.write(json.dumps(logs))
        count = 0
        old_logs = logs
        old_urls = urls

        time.sleep(args.logging_interval)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Push Flink log messages to Logstash by Flink REST API')
    parser.add_argument('db_dir', type=str,
                        help='(Required) If specified, this program keeps pushed last location'
                             'of log messages in this file.')
    parser.add_argument('--ls-addr', type=str,
                        help='Specify Logstash address (host:port).'
                             'If not specified, log messages are printed out.')
    parser.add_argument('--logging-interval', type=int, default=15,
                        help='Polling interval to flink logs in seconds. '
                             'Default is 15 seconds.')
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

    if args.db_dir is not None and not os.path.isdir(args.db_dir):
        logger.error('cannot find', args.db_dir)
        sys.exit(errno.ENOENT)

    for handler in logstash_logger.handlers:
        logstash_logger.removeHandler(handler)
    if args.ls_addr is not None:
        (host, port) = args.ls_addr.split(":")
        logger.info("Logstash: {}:{}".format(host, port))
        requests.TCPLogstashHandler(host, port, version=1)
        logstash_logger.addHandler(logstash.TCPLogstashHandler(host, port, version=1))
        logstash_logger.setFormatter(logging.Formatter("%(message)s"))
    else:
        logstash_logger.addHandler(logging.StreamHandler(stream=sys.stdout))

    push_log_to_logstash(args)