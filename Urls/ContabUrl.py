# coding:utf-8
# @Explain : 说明
# @Time    : 2018/8/9 上午11:02
# @Author  : gg
# @FileName: ContabUrl.py

# 定时器
from Controller.ContabUrl.Index import Index as contab_url_index


#
contab_url_urls = [
    (r"/crontab_url/index\.shtml", contab_url_index),#定时器入口
]
