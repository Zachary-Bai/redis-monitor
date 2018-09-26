#!/bin/env python
# -*- coding:utf-8 -*-

__author__ = 'Zachary Bai'

import json
import time
import socket
import re
import commands
import urllib2
import logging

logger = logging.getLogger(__name__)


class RedisStats:
    # 如果你是自己编译部署到redis，请将下面的值替换为你到redis-cli路径
    _redis_cli = '/usr/bin/redis-cli'
    # _redis_cli = '/opt/apps/redis/src/redis-cli'
    _stat_regex = re.compile(ur'(\w+):([0-9]+\.?[0-9]*)\r')

    def __init__(self, redis_cli, host='127.0.0.1', port='6379', passwd=None):
        if redis_cli:
            self._redis_cli = redis_cli

        self._cmd = '%s -h %s -p %s info' % (self._redis_cli, host, port)
        if passwd not in ['', None]:
            self._cmd = '%s -h %s -p %s -a %s info' % (self._redis_cli, host, port, passwd)

    def stats(self):
        """ Return a dict containing redis stats """
        info = commands.getoutput(self._cmd)
        return dict(self._stat_regex.findall(info))


def main(redis_cli='/usr/bin/redis-cli', redis_list=None):
    if not redis_list:
        logger.warning('not specify redis_list, return')
        return

    ip = socket.gethostname()
    timestamp = int(time.time())
    step = 60
    p = []

    monit_keys = [
        ('connected_clients', 'GAUGE'),
        ('blocked_clients', 'GAUGE'),
        ('used_memory', 'GAUGE'),
        ('used_memory_rss', 'GAUGE'),
        ('total_system_memory', 'GAUGE'),
        ('mem_fragmentation_ratio', 'GAUGE'),
        ('total_commands_processed', 'COUNTER'),
        ('rejected_connections', 'COUNTER'),
        ('expired_keys', 'COUNTER'),
        ('evicted_keys', 'COUNTER'),
        ('keyspace_hits', 'COUNTER'),
        ('keyspace_misses', 'COUNTER'),
        ('keyspace_hit_ratio', 'GAUGE'),
        ('mem_free_ratio', 'GAUGE'),
    ]

    for redis_in in redis_list:
        inst = redis_in.get('redis', None)
        if not inst:
            continue

        host = inst.get('host', '127.0.0.1')
        port = inst.get('port', 6379)
        passwd = inst.get('password', None)
        hostname = inst.get('hostname', ip)
        logger.info('redis host: %s, port: %d, hostname: %s', host, port, hostname)
        metric = "redis"
        endpoint = hostname
        tags = 'port=%d' % port

        try:
            conn = RedisStats(redis_cli, host, port, passwd)
            stats = conn.stats()
        except Exception, e:
            logger.error('connect redis [%s:%d] failed' % (host, port))
            continue

        for key, vtype in monit_keys:
            # 计算命中率
            if key == 'keyspace_hit_ratio':
                try:
                    value = float(stats['keyspace_hits']) / (
                            int(stats['keyspace_hits']) + int(stats['keyspace_misses']))
                except ZeroDivisionError, e:
                    logger.warning('redis [%s:%d] key [%s] calculate failed: %s' % (host, port, key, e))
                    continue
                except:
                    logger.warning('redis [%s:%d] info from dict get key [%s] failed' % (host, port, key))
                    continue

            # 计算空闲内存率
            elif key == 'mem_free_ratio':
                try:
                    value = float(int(stats['total_system_memory']) - int(stats['used_memory'])) / (
                        float(stats['total_system_memory']))
                except ZeroDivisionError, e:
                    logger.warning('redis [%s:%d] key [%s] calculate failed: %s' % (host, port, key, e))
                    continue
                except:
                    logger.warning('redis [%s:%d] info from dict get key [%s] failed' % (host, port, key))
                    continue

            # 碎片率是浮点数
            elif key == 'mem_fragmentation_ratio':
                try:
                    value = float(stats[key])
                except:
                    logger.warning('redis [%s:%d] info from dict get key [%s] failed' % (host, port, key))
                    continue
            else:
                # 一些老版本的redis中info输出的信息很少，如果缺少一些我们需要采集的key就跳过
                if key not in stats.keys():
                    logger.warning('redis [%s:%d] info has no key [%s]' % (host, port, key))
                    continue
                # 其他的都采集成counter，int
                try:
                    value = int(stats[key])
                except:
                    logger.warning('redis [%s:%d] info from dict get key [%s] failed' % (host, port, key))
                    continue

            up_json = {
                'Metric': '%s.%s' % (metric, key),
                'Endpoint': endpoint,
                'Timestamp': timestamp,
                'Step': step,
                'Value': value,
                'CounterType': vtype,
                'TAGS': tags
            }
            p.append(up_json)

    # logger.info(json.dumps(p, sort_keys=True, indent=4))
    method = "POST"
    handler = urllib2.HTTPHandler()
    opener = urllib2.build_opener(handler)
    url = 'http://%s:1988/v1/push' % '127.0.0.1'
    request = urllib2.Request(url, data=json.dumps(p))
    request.add_header("Content-Type", 'application/json')
    request.get_method = lambda: method
    try:
        connection = opener.open(request)
    except urllib2.HTTPError, e:
        connection = e
    except Exception, e:
        logger.error('redis monitor metrics push failed: %s' % e)
        return

    # check. Substitute with appropriate HTTP code.
    if connection.code == 200:
        logger.info('data push result: %s' % connection.read())
    else:
        logger.error('{"err":1,"msg":"%s"}' % connection)


def start(redis_yml=None):
    if not redis_yml:
        logger.warning('redis_yml not given, will exit')
        exit(1)

    redis_cli = redis_yml.get('client', '/usr/bin/redis-cli')
    redis_list = redis_yml.get('redis_to_monitor', None)
    if not redis_list:
        logger.warning('not specify redis server to monitor, will exit')
        exit(1)
    # proc = commands.getoutput(' ps -ef|grep %s|grep -v grep|wc -l ' % os.path.basename(sys.argv[0]))
    # sys.stdout.flush()
    # if int(proc) < 5:
    main(redis_cli, redis_list)


if __name__ == '__main__':
    start()
