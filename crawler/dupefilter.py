# coding:utf-8
import pyreBloom


class Dupefilter(object):
    def __init__(self, crawler):
        self.crawler = crawler

    @classmethod
    def from_crawler(cls, crawler):
        return cls(crawler)

    def open(self, spider_name):
        kw = self.crawler.redis.connection_pool.connection_kwargs
        host = kw['host']
        port = kw['port']
        db = kw['db']
        self.bfilter = pyreBloom.pyreBloom("bf:%s" % spider_name, 100000000, 0.001,
                                           host=host, port=port, db=db)

    def seen(self, req):
        return req.url in self.bfilter

    def do_fingerprint(self, req):
        if req.method == "GET":
            self.bfilter.add(req.url)

    def close(self):
        self.bfilter.delete()
