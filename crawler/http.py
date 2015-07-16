# coding:utf-8
import re
import json

from lxml import html
from crawler.exceptions import EmptyExtractException


class Request(object):
    def __init__(self, url=None, method="GET", callback=None, **kw):
        self.url = url
        self.method = method
        self.data = kw.get("data")
        self.meta = kw.get("meta", {})
        self.callback = callback or 'parse'
        self.headers = kw.get("headers")
        self.verify = kw.get("headers", True)
        self.kw = kw

    def to_json(self):
        if callable(self.callback):
            cb = self.callback.__name__
        else:
            cb = str(self.callback)
        d = {'url': self.url, 'method': self.method, 'data': self.data,
             'meta': self.meta, 'callback': cb, 'headers': self.headers}
        d.update(self.kw)

        return json.dumps(d)

    @classmethod
    def from_json(cls, j):
        obj = cls()
        for k, v in json.loads(j).items():
            obj.__setattr__(k, v)

        return obj


class Response(object):
    def __init__(self, url=None, content=None, **kw):
        self.url = url
        self.content = content or ""
        self.request = kw.get("request")
        self.headers = kw.get("headers")
        self.obj = kw.get("obj")
        self.meta = kw.get("meta", {})
        self.code = kw.get("code")
        self.root = html.fromstring(self.content)

    def xpath(self, path):
        return Extractor(self.root).xpath(path)

    def json_load(self):
        return json.loads(self.content)


class Extractor(object):
    def __init__(self, root, path=None, data=None):
        self.path = path
        self.root = root
        self.data = data

    def xpath(self, path):
        elist = ExtractList()
        for el in self.root.xpath(path):
            if isinstance(el, html.HtmlElement):
                data = html.tostring(el, encoding='unicode')
            else:
                data = el

            elist.append(Extractor(el, path=path, data=data))

        return elist

    def fetch(self):
        return [html.tostring(self.root, encoding='unicode')
                if isinstance(self.root, html.HtmlElement) else self.root, ]

    def re(self, regex):
        s = html.tostring(self.root, encoding='unicode') if isinstance(self.root, html.HtmlElement) else self.root
        return re.findall(regex, s)


class ExtractList(list):
    def xpath(self, path):
        return ExtractList(el.xpath(path) for el in self)

    def fetch(self):
        return [el.fetch()[0] for el in self]

    def first(self):
        return self[0].fetch()

    def iter(self):
        for el in self:
            yield el.fetch()[0]

    def re(self, regex):
        return [el.re(regex) for el in self]
