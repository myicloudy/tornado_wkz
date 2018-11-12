# coding:utf-8

from Controller.Message.CommonMessage import CommonMessage
import tornado
import logging
import logging.config
# 加载前面的标准配置
from Config.logging import LOGGING
import time
logging.config.dictConfig(LOGGING)

import aiohttp
import asyncio
# import pydf
# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class Index(CommonMessage):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return: 
        '''
        pass

    def get(self):
        self.write('CommonMessageindex')

