#!/usr/bin/env python
# coding:utf-8
import random
import inspect
import urlparse
import traceback
from collections import Iterable
from pkgutil import iter_modules

import gevent
from gevent import Greenlet

from crawler.http import Request, Response
from crawler.queue import Empty


class Spider(Greenlet):
    name = None
    allowed_domains = []
    start_urls = []

    def __init__(self, crawler=None):
        Greenlet.__init__(self)
        self.crawler = crawler
        self.redis = crawler.redis
        self.session = crawler.session

        self.spider_queue = self.crawler.spider_queue
        self.schedule_queue = self.crawler.schedule_queue
        self.running = False
        self.log = crawler.log

    @classmethod
    def from_crawler(cls, crawler):
        obj = cls(crawler)
        return obj

    def fetch(self):
        while not self.crawler.event.is_set():
            try:
                request = self.spider_queue.get()
            except Empty:
                gevent.sleep(random.random())
                continue
            if not self._domain_allow(request.url):
                self.log.warn("Url out of domain: {}".format(request.url))
                continue
            self.crawler.running = True
            try:
                resp = self.session.request(request.method, request.url,
                                            data=request.data,
                                            headers=request.headers,
                                            allow_redirects=True,
                                            verify=request.verify,
                                            timeout=30)
                response = Response(url=resp.url, content=resp.text,
                                    request=request, code=resp.status_code,
                                    headers=resp.headers, meta=request.meta,
                                    obj=resp)
                meta_refresh = response.xpath("//meta[@http-equiv='refresh']/@content").re(".*?url=(.*)$")
                if meta_refresh:
                    self.log.debug("(meta refresh) %s" % meta_refresh[0])
                    resp = self.session.get(meta_refresh[0], allow_redirects=True)
                    response = Response(url=resp.url, content=resp.text,
                                        request=request, code=resp.status_code,
                                        headers=resp.headers, meta=request.meta,
                                        obj=resp)

                # registry fingeprint for request url.
                self.crawler.fp.do_fingerprint(request)

                reqs = self._extra(response)
                if reqs:
                    if isinstance(reqs, Iterable):
                        for req in reqs:
                            if isinstance(req, Request):
                                self.schedule_queue.put(req)
                    else:
                        if isinstance(reqs, Request):
                            self.schedule_queue.put(reqs)

                self.log.debug("(%d) %s %s" % (response.code, response.request.method, response.url))
            except Exception as e:
                self.log.error("Url fetch request error: %s" % request.url)
                traceback.print_exc()

            self.crawler.running = False
            gevent.sleep(random.random())

    def _extra(self, response):
        callback = response.request.callback
        if callback:
            if callable(callback):
                return callback(response)
            elif type(callback) in (str, unicode):
                cb = self.__getattribute__(callback)
                return cb(response)
        else:
            return self.parse(response)

    def _domain_allow(self, url):
        if self.allowed_domains:
            return urlparse.urlparse(url).netloc in self.allowed_domains
        else:
            return True

    def parse(self, response):
        urls = response.xpath("//a/@href").fetch()
        for url in urls:
            if not url.startswith("http:"):
                url = self.abs_url(response, url)
            yield Request(url)

    def abs_url(self, response, url):
        if isinstance(response, str):
            prefix = response
        else:
            prefix = response.url
        return urlparse.urljoin(prefix, url)

    def _run(self):
        self.fetch()


class SpiderManager(object):
    def __init__(self):
        self.spider_module = 'spiders'
        self._spiders = {}
        for module in self.walk_modules(self.spider_module):
            self._filter_spiders(module)

    def _filter_spiders(self, module):
        for spcls in self.iter_spider_classes(module):
            self._spiders[spcls.name] = spcls

    def iter_spider_classes(self, module):
        for obj in vars(module).itervalues():
            if inspect.isclass(obj) and \
                    issubclass(obj, Spider) and \
                    obj.__module__ == module.__name__ and \
                    getattr(obj, 'name', None):
                yield obj

    def walk_modules(self, path, load=False):
        mods = []
        mod = __import__(path, {}, {}, [''])
        mods.append(mod)
        if hasattr(mod, '__path__'):
            for _, subpath, ispkg in iter_modules(mod.__path__):
                fullpath = path + '.' + subpath
                if ispkg:
                    mods += self.walk_modules(fullpath)
                else:
                    submod = __import__(fullpath, {}, {}, [''])
                    mods.append(submod)
        return mods

    def create(self, spider_name):
        try:
            spcls = self._spiders[spider_name]
        except KeyError:
            raise KeyError("Spider not found: %s" % spider_name)

        return spcls

    def get_list(self):
        return self._spiders.keys()
