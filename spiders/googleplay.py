# coding:utf-8
import re

import redis

from crawler.spider import Spider
from crawler.http import Request


class GoogleplaySpider(Spider):
    name = 'googleplay'
    start_urls = ['https://play.google.com/store/apps/', ]

    def __init__(self, *args, **kvargs):
        Spider.__init__(self, *args, **kvargs)
        self.redis = redis.Redis(host='localhost', db=0)

    def parse(self, response):
        urls = response.xpath("//a/@href").fetch()
        for url in urls:
            match = re.search(".*/details\?id=(.*)$", url)
            if match and '&' not in url and '.' in url:
                appid = match.group(1)
                if not self.redis.hexists("app_record", appid):
                    self.redis.rpush('appids', appid)

            if not url.startswith('http'):
                url = self.abs_url("https://play.google.com", url)
            yield Request(url)
