#coding:utf-8
import logging

class Logger(object):
    def __init__(self):
        self.handler = logging.StreamHandler()
        formatter = logging.Formatter("%(asctime)s [%(name)s]-%(levelname)s: %(message)s")
        self.handler.setFormatter(formatter)

    def getlog(self, logger="crawler", level=None):
        logger = logging.getLogger(logger)
        if level:
            logger.setLevel(level)
        else:
            logger.setLevel(logging.DEBUG)

        logger.addHandler(self.handler)
        return logger
