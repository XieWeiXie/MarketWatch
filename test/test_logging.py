#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2017/1/3 13:30
"""
import logging

logger = logging.getLogger("test_logging")
logger.setLevel(logging.DEBUG)

ch = logging.StreamHandler()
ch.setLevel(logging.NOTSET)

formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch.setFormatter(formatter)
logger.addHandler(ch)

logger.debug("0")
logger.info("1")
logger.warn("2")
logger.error("3")
logger.critical("4")


import logging
import logging.config
logging.config.fileConfig("logging_config.ini")

logger2 = logging.getLogger("sample")
logger2.debug("00")
logger2.info("01")
logger2.warn("02")
logger2.error("03")
logger2.critical("04")


import logging
import logging.config

config = {
    'version': 1,
    'formatters': {
        'simple': {
            'format': '%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        },
    },
    'handlers': {
        'console': {
            'class': 'logging.StreamHandler',
            'level': 'DEBUG',
            'formatter': 'simple'
        },
        'file': {
            'class': 'logging.FileHandler',
            'filename': 'logging.log',
            'level': 'DEBUG',
            'formatter': 'simple'
        },
    },
    'loggers':{
        'root': {
            'handlers': ['console'],
            'level': 'DEBUG',
            # 'propagate': True,
        },
        'simple': {
            'handlers': ['console', 'file'],
            'level': 'WARN',
        }
    }
}

logging.config.dictConfig(config)


print 'logger:'
logger = logging.getLogger('root1')

logger.debug('debug message')
logger.info('info message')
logger.warn('warn message')
logger.error('error message')
logger.critical('critical message')


print 'logger2:'
logger2 = logging.getLogger('simple1')

logger2.debug('debug message')
logger2.info('info message')
logger2.warn('warn message')
logger2.error('error message')
logger2.critical('critical message')

import logging
from log4mongo.handlers import MongoHandler

logger = logging.getLogger('mongo_example')

mon = MongoHandler(host='localhost', database_name='db')
mon.setLevel(logging.WARNING)

ch = logging.StreamHandler()
ch.setLevel(logging.ERROR)

logger.addHandler(mon)
logger.addHandler(ch)


logger.debug('debug message')
logger.info('info message')
logger.warn('warn message')
logger.error('error message')
logger.critical('critical message')