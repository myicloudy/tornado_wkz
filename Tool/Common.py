# coding:utf-8
# @Explain : 常用工具类
# @Time    : 2018/08/27 上午11:31
# @Author  : Wangwenhao
# @FileName: Common.py

import random
import datetime
import time
import string
import re

class Common():

    '''
       时间类
    '''
    # 随机生成0-9指定位数的数字
    def random_number(self, value):
        return ''.join(random.choice("0123456789") for i in range(value))

    # 获取当前时间 带-间隔符
    def current_time(self):
        return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # 获取当前时间 不带-间隔符
    def current_time_out(self):
        return datetime.datetime.now().strftime('%Y%m%d%H%M%S')

    # 获取当前时间戳
    def current_stamp(self):
        return int(time.time())

    # 获取过去N个小时时间
    def past_hour_time(self, value):
        return (datetime.datetime.now()-datetime.timedelta(hours=value)).strftime('%Y-%m-%d %H:%M:%S')

    # 获取过去/未来N天时间
    def future_day_time(self, value):
        return (datetime.datetime.now()+datetime.timedelta(days=value)).strftime('%Y-%m-%d %H:%M:%S')


    '''
       字符串类
    '''
    # 随机生成指定位数的字符串
    def random_str(self, value):
        return ''.join(random.sample(string.ascii_letters + string.digits, value))


    '''
       正则验证类
    '''
    # 手机号码正则校验
    def re_phone(self, phone):
        phone_pat = re.compile('^(13\d|14[5|7]|15\d|166|17[3|6|7]|18\d)\d{8}$')
        res = re.search(phone_pat, phone)
        if res:
            print('正常手机号')
        else:
            print('不是手机号')