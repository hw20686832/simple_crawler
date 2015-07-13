#!/usr/bin/env python
# coding:utf-8
import sys
from gevent import monkey
monkey.patch_all()
import logging
from optparse import OptionParser

import gevent
from gevent import queue
from gevent.event import Event
from gevent.pool import Pool
import redis
import requests

import settings
from crawler.queue import Queue
from crawler.http import Request
from crawler.scheduler import Schedule
from crawler.dupefilter import Dupefilter
from crawler.spider import SpiderManager
from utils.logger import Logger
from utils.tools import class_import


class Crawler(object):
    def __init__(self):
        self.concurrent_num = settings.CONCURRENT_NUM
        self.spider_pool = Pool(self.concurrent_num)
        self.queue_key = ''

        self.session = requests.Session()
        self.session.headers = {"Accept:text/html": "application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                                "Accept-Encoding": "gzip,deflate,sdch",
                                "Accept-Language": "zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4",
                                "Cache-Control": "max-age=0",
                                "Connection": "keep-alive",
                                "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"}

        self.redis = redis.Redis(host=settings.REDIS_HOST,
                                 port=settings.REDIS_PORT,
                                 db=settings.REDIS_DB)

    def install(self, options):
        self.event = Event()
        sm = SpiderManager()
        self.spider_class = sm.create(options.spider)
        spider_qk = "spider:q:%s" % self.spider_class.name
        schedule_qk = "schedule:q:%s" % self.spider_class.name

        self.log = Logger().getlog(level=logging.getLevelName(options.level))
        self.spider_queue = Queue(spider_qk, self.redis)
        self.schedule_queue = Queue(schedule_qk, self.redis)
        self.pipeline_queue = queue.Queue()

        self.fp = Dupefilter.from_crawler(self)
        self.fp.open(self.spider_class.name)

        self.forever = options.forever
        self.running = False
        if options.urls:
            self.addition_urls = options.urls.split(',')
        else:
            self.addition_urls = []
        self.schedule = Schedule(self)

    def do_pipeline(self, spider):
        pipelines = [class_import(ppclass).from_crawler(self)
                     for ppclass in spider.pipelines]
        while True:
            try:
                obj = self.pipeline_queue.get()
            except queue.Empty:
                gevent.sleep(1)
            else:
                for ppl in pipelines:
                    obj = ppl.process(obj)

    def run(self):
        if len(self.schedule_queue) == 0 and len(self.spider_queue) == 0:
            for url in set(self.spider_class.start_urls + self.addition_urls):
                req = Request(url)
                self.schedule_queue.put(req)

        for i in xrange(self.concurrent_num):
            spd = self.spider_class.from_crawler(self)
            self.spider_pool.start(spd)

        gevent.spawn(self.schedule.start).join()
        self.log.info("Crawl finish.")

    def _even(self):
        return self.forever or (
            self.spider_queue.qlen() > 0 or self.schedule_queue.qlen() > 0) \
            or self.running

    def stop(self):
        self.event.set()
        self.fp.close()


def gen_spider(spider, domain):
    pm = SpiderManager()
    if spider in pm.get_list():
        raise Exception(u"指定的Spider已经存在.")
    default_filename = "spiders/%s.py" % spider
    with open("utils/spider.tpl") as t:
        content = t.read()

    content %= {'Spider': spider.title(), 'spider': spider, 'domain': domain}
    with open(default_filename, "w") as f:
        f.write(content)

    print u"Spider已经创建在 %s" % default_filename


def list_spiders():
    sm = SpiderManager()
    print ', '.join(sm.get_list())


def main():
    usage = "Usage: %prog [run|gen|list] [options] arg"
    parser = OptionParser(usage)
    try:
        cmd = sys.argv[1]
    except IndexError:
        parser.error("incorrect number of arguments")

    if cmd == "run":
        parser.add_option("-s", "--spider", dest="spider",
                          help="give a spider")
        parser.add_option("-u", "--urls", dest="urls",
                          help="give some urls, split with ','")
        parser.add_option("-L", "--logfile", dest="logfile",
                          help="logging file for crawler")
        parser.add_option("-l", "--loglevel", dest="level", default="DEBUG",
                          help="logging level for crawler")
        parser.add_option("", "--forever", dest="forever", action="store_true",
                          default=False, help="run crawler forever")
        (options, args) = parser.parse_args(args=sys.argv[1:])
        crawl = Crawler()
        crawl.install(options)
        crawl.run()
    elif cmd == "gen":
        parser.add_option("-s", "--spider", dest="spider",
                          help="given a spider.")
        parser.add_option("-d", "--domain", dest="domain",
                          help="given a domain that you will crawl.")
        (options, args) = parser.parse_args(args=sys.argv[1:])
        gen_spider(options.spider, options.domain)
    elif cmd == "list":
        list_spiders()
    else:
        parser.error("unknow argument '%s'" % cmd)


if __name__ == '__main__':
    main()
