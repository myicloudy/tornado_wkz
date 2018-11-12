# coding:utf-8
# @Explain : 说明
# @Time    : 2018/7/11 下午3:14
# @Author  : gg
# @FileName: Analysis.py

# 分析器
from Controller.Analysis.Index import Index as analysis_index
from Controller.Analysis.ReadLog import Index as analysis_readlog

#
analysis_urls = [
    (r"/analysis/index\.shtml", analysis_index),
    (r"/analysis/read_log\.shtml", analysis_readlog),
]
