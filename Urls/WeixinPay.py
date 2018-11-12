# coding:utf-8

# 前台
from Controller.WeixinPay.Wxjsapi import wxjsapi
from Controller.WeixinPay.Wxjsapi import  wxjsapiReturn as wxjsapi_return
from Controller.WeixinPay.Wxjsapi import wxjsapiSuccess as wxjsapiSuccess
from Controller.WeixinPay.Wxjsapi import wxjsapiError as wxjsapiError
#
WeixinPay_urls = [
    (r"/wxjsapi\.shtml", wxjsapi),
    (r"/wxjsapi_return\.shtml", wxjsapi_return),
    (r'/wxjsapi_success\.shtml',wxjsapiSuccess),
    (r'/wxjsapi_error\.shtml',wxjsapiError)
]
