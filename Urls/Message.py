# coding:utf-8

# 后台
from Controller.Message.Index import Index as message_index
from Controller.Message.Kproducer import Kproducer as message_Kproducer

message_urls = [
    (r"/message\.shtml", message_index),
    (r"/kproducer\.shtml", message_Kproducer),

]
