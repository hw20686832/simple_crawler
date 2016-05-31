# coding:utf-8
import gevent

import settings
from crawler.queue import TrafficQueue, TrafficLimit


class Schedule(object):
    def __init__(self, crawler):
        self.crawler = crawler
        self.log = crawler.log
        self.spider_queue = crawler.spider_queue
        self.schedule_queue = crawler.schedule_queue

        self.traffic = TrafficQueue(settings.REQUEST_LIMIT)

    def start(self):
        self._do_schedule()
        self.crawler.stop()

    def _do_schedule(self):
        while self.crawler._even():
            if self.spider_queue.qlen() <= settings.MAX_REQUEST:
                qfree = min(settings.MAX_REQUEST - self.spider_queue.qlen(),
                            self.schedule_queue.qlen())
                n = qfree
                while n:
                    request = self.schedule_queue.get()
                    if not self.crawler.fp.seen(request):
                        try:
                            if request.meta.get('limit'):
                                self.traffic.t_in(request)
                        except TrafficLimit:
                            next_req = self.schedule_queue.get()
                            self.schedule_queue.insert(request)
                            request = next_req
                        finally:
                            self.spider_queue.put(request)
                            # registry fingeprint for request url.
                            self.crawler.fp.do_fingerprint(request)

                    n -= 1
                self.log.info("schedule %d, spider %d" %
                              (self.schedule_queue.qlen(), self.spider_queue.qlen()))

            gevent.sleep(5)
