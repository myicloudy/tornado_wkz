# coding:utf-8
# @Explain : 支付
# @Time    : 2018/9/4 下午2:05
# @Author  : gg
# @FileName: Pay.py

from Controller.Pay.CreateOrder import CreateOrder as pay_CreateOrder

pay_urls = [
    (r"/pay/create_order", pay_CreateOrder),


]
