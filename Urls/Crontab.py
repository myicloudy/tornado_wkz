# coding:utf-8
# @Explain : 说明
# @Time    : 2018/08/10
# @Author  : mzf
# @FileName: Crontab.py

# linux Crontab
from Controller.Crontab.Index import InsertHandler as crontab_inster
from Controller.Crontab.Index import QueryHandler as crontab_query
from Controller.Crontab.Index import UpdateHandler as crontab_update
from Controller.Crontab.Index import DelHandler as crontab_del

contab_URL_urls = [
    (r"/crontab/write", crontab_inster),#写入
    (r"/crontab/query", crontab_query),#查询
    (r"/crontab/update", crontab_update),#更新
    (r'/crontab/delete', crontab_del),#删除
]