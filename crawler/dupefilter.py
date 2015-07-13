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
        self.filter = pyreBloom.pyreBloom("bloomfilter", 100000000, 0.001,
                                          host=host, port=port)

    def seen(self, req):
        return req.url in self.filter

    def do_fingerprint(self, req):
        if req.method == "GET":
            self.filter.add(req.url)

    def close(self):
        pass
