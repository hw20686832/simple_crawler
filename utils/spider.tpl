# coding:utf-8
from crawler.spider import Spider


class %(Spider)sSpider(Spider):
    name = '%(spider)s'
    start_urls = ['%(domain)s', ]

    def parse_item(self, response):
        pass