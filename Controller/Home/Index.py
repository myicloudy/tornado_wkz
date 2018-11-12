# coding:utf-8

from Controller.Home.CommonHome import CommonHome
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
class Index(CommonHome):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return: 
        '''
        pass

    def get(self):
        self.write('index')

