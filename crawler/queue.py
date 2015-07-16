# coding:utf-8
import time

import redis

import settings
from crawler.http import Request
from crawler.exceptions import TrafficLimit, Empty


class TrafficQueue(object):
    def __init__(self, num, ctime=60):
        self.num = num
        self.ctime = ctime

        self.tq = []

    def t_in(self, req):
        self._auto_remove()
        if len(self.tq) <= self.num:
            self.tq.append((req, time.time()))
        else:
            raise TrafficLimit("This request was out of limit (%d/%d)" %
                               (self.num, self.ctime))

        return req

    def _auto_remove(self):
        self.tq = filter(lambda x: time.time()-x[1] < self.ctime, self.tq)

    def t_len(self):
        return len(self.tq)


class Queue(object):
    def __init__(self, key, rd=None):
        if not rd:
            rd = redis.Redis(host=settings.REDIS_HOST,
                             port=settings.REDIS_PORT,
                             db=settings.REDIS_DB)

        self.redis = rd
        self.key = key

    def get(self):
        sq = self.redis.lpop(self.key)
        if sq:
            return Request.from_json(sq)
        else:
            raise Empty("Queue empty")

    def mget(self, num=1):
        for i in range(num):
            yield self.get()

    def put(self, req):
        self.redis.rpush(self.key, req.to_json())

    def qlen(self):
        return len(self)

    @property
    def next_req(self):
        return self.redis.lrange(self.key, -1, -1)[0]

    def insert(self, req):
        self.redis.lpush(self.key, req.to_json())

    def drop(self):
        self.redis.delete(self.key)

    def __len__(self):
        return self.redis.llen(self.key)
