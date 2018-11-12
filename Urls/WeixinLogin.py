# coding:utf-8

# 前台
from Controller.WeixinLogin.wxlogin import wxlogin, wxAuthorize

#
WeixinLogin_urls = [
    (r"/wxlogin\.shtml", wxlogin),
    (r"/wxAuthorize\.shtml", wxAuthorize),
]
