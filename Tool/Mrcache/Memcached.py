# coding:utf-8
# @Explain : 说明
# @Time    : 2018/6/19 上午10:36
# @Author  : gg
# @FileName: Memcached.py

import configparser

conf = configparser.ConfigParser()
#因为用comsumer调用时的路径问题，用tornado框架默认从根目录寻目录
try:
    conf.read("../../Config/mall.conf")
    conf.get('memcached', 'host')
except:
    conf.read("Config/mall.conf")
    conf.get('memcached', 'host')
import memcache
memstr = conf.get('memcached', 'host') + ':' + conf.get('memcached', 'port')
class pyMemcached():
    def create(self):
        mc = memcache.Client([memstr], debug=conf.get('memcached', 'debug'))
        return mc


