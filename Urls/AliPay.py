'''
 @Author:    wwh
 @Create Date:   2018-08-16
 @Description:   支付宝支付路由
'''
# coding:utf-8

# 交易支付
from Controller.AliPay.AliPay import Pay
# 交易成功回调通知
from Controller.AliPay.AliPay import PayNotifyUrl
# 交易状态查询
from Controller.AliPay.AliPay import Query
# 交易退款
from Controller.AliPay.AliPay import Refund
# 支付发起页面展示
from Controller.AliPay.AliPay import alipayRequest as alipayRequest
# 支付成功页面展示
from Controller.AliPay.AliPay import PaySuccess as PaySuccess
# 支付失败页面展示
from Controller.AliPay.AliPay import PayFail as PayFail

AliPay_urls = [
    (r"/alipay\.shtml", Pay),
    (r"/alipayNotifyUrl\.shtml", PayNotifyUrl),
    (r'/aliquery\.shtml', Query),
    (r'/alirefund\.shtml', Refund),
    (r'/alipayRequest\.shtml', alipayRequest),
    (r'/alipaySuccess\.shtml', PaySuccess),
    (r'/alipayFail\.shtml', PayFail),
]
