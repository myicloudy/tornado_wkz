# coding:utf-8

# 前台
from Controller.WeixinUpdate.Index import Index as WeixinUpdate_index
from Controller.WeixinUpdate.CronSendMessage import Index as home_CronSendMessage
from Controller.WeixinUpdate.WxUpdateProducer import WxUpdateProducer as WxUpdateProducer

#
WeixinUpdate_urls = [
    (r"/wxupdate\.shtml", WeixinUpdate_index),  # 微信更新粉丝
    (r"/CronSendMessage\.shtml", home_CronSendMessage),  # 实时发送消息模块
    (r"/wx_producer\.shtml", WxUpdateProducer),  # 微信更新生产者
]
