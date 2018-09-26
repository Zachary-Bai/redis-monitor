#!/usr/bin/env python
# -*- coding: utf-8 -*-

""" main class """

__author__ = 'Zachary Bai'

import yaml
import logging.config
import os
import schedule
import time
from monitor import redis_monitor

with open('./logging.yml') as f:
    D = yaml.load(f)
    D.setdefault('version', 1)
    logging.config.dictConfig(D)

logger = logging.getLogger('main')


def load_redis_yml():
    with open('./redis_to_monitor.yml') as f:
        redis_to_mon = yaml.load(f)
    return redis_to_mon


_redis_to_mon = load_redis_yml()


def timer():
    schedule.every().minute.do(task)
    while True:
        schedule.run_pending()
        # 每隔1s做一次检测
        time.sleep(1)


def task():
    pwd = os.getcwd()
    if pwd.endswith('/'):
        pwd = pwd[:len(pwd)-1]
    os.chdir('%s/monitor' % pwd)
    global _redis_to_mon
    redis_monitor.start(_redis_to_mon)
    os.chdir(pwd)


if __name__ == '__main__':
    timer()
    # task()
