# coding:utf-8
from .Home import home_urls
from .Test import test_urls
from .Admin import admin_urls
from .WeixinLogin import WeixinLogin_urls
from .WeixinPay import WeixinPay_urls
from .Analysis import analysis_urls
from .WeixinUpdate import WeixinUpdate_urls
from Controller.Control import ErrorHandler
from .Message import message_urls
from .ContabUrl import contab_url_urls
from .InsiteApi import insite_api_urls
from .AliPay import AliPay_urls
from .Pay import pay_urls
error_urls = [(r"/.*", ErrorHandler)]
urls = test_urls + home_urls + admin_urls + WeixinLogin_urls + WeixinPay_urls + analysis_urls + WeixinUpdate_urls \
       + message_urls + contab_url_urls + AliPay_urls + insite_api_urls + pay_urls+ error_urls
