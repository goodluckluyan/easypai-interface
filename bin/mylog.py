#! /usr/bin/python
# encoding:utf-8

# 日志
import logging
logfile="d:\\Project\\easypai\\bin\\log.log"
logger = logging.getLogger()
logger.setLevel(logging.INFO)
fh = logging.FileHandler(logfile,mode='a')
fh.setLevel(logging.INFO)
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)

formatter = logging.Formatter("%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s")
fh.setFormatter(formatter)
ch.setFormatter(formatter)

logger.addHandler(fh)
logger.addHandler(ch)
