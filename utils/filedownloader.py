# coding:utf-8
from __future__ import division
import os
import string

import requests

default_headers = {"Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                   "Accept-Encoding": "gzip,deflate,sdch",
                   "Accept-Language": "zh-CN,zh;q=0.8,en;q=0.6,zh-TW;q=0.4",
                   "Cache-Control": "max-age=0",
                   "Connection": "keep-alive",
                   "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Ubuntu Chromium/36.0.1985.125 Chrome/36.0.1985.125 Safari/537.36"}


def download(url, filename, **kwargs):
    method = kwargs.get('method', 'GET')
    headers = kwargs.get('headers', default_headers)
    data = kwargs.get('data')
    cookies = kwargs.get('cookies')
    timeout = kwargs.get('timeout', 60)
    session = kwargs.get('session', requests)

    is_binary = False
    with open(filename, 'wb') as handle:
        response = session.request(method, url, data=data,
                                   headers=headers, cookies=cookies,
                                   stream=True, timeout=timeout)
        for block in response.iter_content(512):
            if not block:
                break

            if not is_binary:
                text_characters = "".join(map(chr, range(32, 127)) + list("\n\r\t\b"))
                _null_trans = string.maketrans("", "")
                if "\0" in block:
                    is_binary = True

                t = block.translate(_null_trans, text_characters)
                if len(t)/len(block) > 0.30:
                    is_binary = True
                else:
                    break
            handle.write(block)

    if not is_binary:
        os.remove(filename)

    return {'result': is_binary,
            'data': {'filename': filename.rsplit('/', 1)[-1]}}
