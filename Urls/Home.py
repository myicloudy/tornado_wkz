# coding:utf-8

# 前台
from Controller.Home.Index import Index as home_index
from Controller.Home.Apitest import Apitest as home_apitest
#
home_urls = [
    (r"/", home_index),
    (r"/index\.shtml", home_index),  # 首页
    (r"/apitest", home_apitest),  # apitest

]
