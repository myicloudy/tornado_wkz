# coding:utf-8
# @Explain : 说明
# @Time    : 2018/7/16 下午3:29
# @Author  : gg
# @FileName: baidu.py
from Controller.Test.CommonTest import CommonTest
import logging
import logging.config
# 加载前面的标准配置
from Config.logging import LOGGING
import time
import re
import json
import requests
import tornado
logging.config.dictConfig(LOGGING)
class Index(CommonTest):
    def get(self):
        url = 'http://www.baidu.com/s?wd=' + self.get_argument("wd",'沃享')
        self.write('<a href="' + url + '" target="_blank">测试抓取地址</a><br><br><br>')
        http_body = requests.get(url).content  # 获取到body数据
        retArr = re.findall(b"data-tools='(.+?)'", http_body)  # 正则匹配所有数据
        for x in retArr:
            ss = json.loads(x)
            self.write('<a href="' + ss['url'] + '" target="_blank">' + ss['title'] + '</a><br><br>')
