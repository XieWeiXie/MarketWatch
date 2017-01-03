#!/usr/bin/env python
# encoding: utf-8

"""
@author: paul.xie
@software: PyCharm
@time: 2017/1/3 13:50
"""
import logging


def get_logger(to_console=True, to_file=False):
    _logger = logging.getLogger(__name__)
    _logger.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    if to_console:
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(formatter)
        _logger.addHandler(console)

    if to_file:
        file_handler = logging.FileHandler("logging.log")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        _logger.addHandler(file_handler)
    return _logger
logger = get_logger(to_console=True, to_file=False)