# coding:utf-8

import logging.config
import json
# 加载前面的标准配置
from Config.logging import LOGGING
from Controller.WeixinPay.CommonWeixinPay import CommonWeixinPay

logging.config.dictConfig(LOGGING)
from Tool.WeixinGG.WeixinPay import WeixinPay


appId = 'wx787f04067ec0c38d'
appSecret = 'e238f6b59fb8c417c278c2b76af5fff7'
mch_id = '1380486802'
mch_key = 'fc3f5197b65e7a38924ee95cc0deb80e'
notify_url = 'http://dayu.mingmore.com/wxjsapi_return.shtml'
import urllib

from decimal import Decimal

import time


# import pydf
# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg

class wxjsapi(CommonWeixinPay):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return: 
        '''
        # 检查微信登录
        bid = self.DefaultValues['bid']
        self.wxInfo = self.checkWeixinLogin(bid, True)

    '''
    发起微信jsapi升支付
    '''

    def get(self):

        bid = self.DefaultValues['bid']
        userId = self.wxInfo['userId']
        orderId = self.get_argument('orderId', None)
        if not orderId:
            self.redirect('/user.shtml')
        # 订单表
        # orderFind = self.db.query(online_order).filter(online_order.id == orderId,
        #                                                online_order.userId == userId).first()
        orderFind=None
        if not hasattr(orderFind, 'id'):
            self.redirect('/user.shtml')

        wxClass = WeixinPay(bid, app_id=appId, mch_id=mch_id, mch_key=mch_key, notify_url=notify_url)
        # out_trade_no = wxClass.out_trade_no
        ip = self.get_remote_ip or '119.137.54.172'
        # ip='119.137.54.172'
        openid = self.wxInfo['openid']
        total_fee = int(float(Decimal(orderFind.orderPrice)) * 100)
        out_trade_no = str(Decimal(orderFind.orderNum))
        # self.write(out_trade_no)
        raw = wxClass.jsapi(openid=openid, body=u"www", out_trade_no=out_trade_no, total_fee=total_fee,
                            spbill_create_ip=ip)
        # print(raw)
        #
        self.write(json.dumps(raw))

        # raw = {'package': 'prepay_id=wx22164515725806de16a63d443050562273', 'appId': 'wx787f04067ec0c38d',
        #        'signType': 'MD5', 'timeStamp': '1529657115', 'nonceStr': 'gNDHECBm6XQjhbOGExbeXGxQfkWFCjtB',
        #        'sign': '3960911C9DA941DE3E9FB8FF8B8E89B9'}
        # raw.update(paySign=raw.pop('sign'))
        rawJson = json.dumps(raw)
        # self.write('wxjspai')
        self.render('WeixinPay/weixinJsPay.html', raw=raw, rawJson=rawJson)


class wxjsapiReturn(CommonWeixinPay):
    def check_xsrf_cookie(self):
        # 关闭xsrf
        pass

    def get(self, *args, **kwargs):
        '''
        微信jsapi支付回调 走post
        :return: 
        '''
        # self.AppLogging.info('get---%s' % self.request.body)
        bid = self.DefaultValues['bid']
        wxClass = WeixinPay(bid, app_id=appId, mch_id=mch_id, mch_key=mch_key, notify_url=notify_url)
        self.write(wxClass.reply("OK", True))
        # xml = '''<xml><return_code><![CDATA[%s]]></return_code><return_msg><![CDATA[%s]]></return_msg></xml>'''
        # # self.write('hehe')
        # self.write(xml % ('SUCCESS', 'OK'))
        pass

    def post(self, *args, **kwargs):
        '''
        微信jsapi支付回调 走post
        :param args: 
        :param kwargs: 
        :return: 
        '''
        bid = self.DefaultValues['bid']
        wxClass = WeixinPay(bid, app_id=appId, mch_id=mch_id, mch_key=mch_key, notify_url=notify_url)
        self.AppLogging.info('post---%s' % self.request.body)
        data = self.request.body
        data = '<xml><appid><![CDATA[wx787f04067ec0c38d]]></appid>\n<bank_type><![CDATA[CFT]]></bank_type>\n<cash_fee><![CDATA[1]]></cash_fee>\n<fee_type><![CDATA[CNY]]></fee_type>\n<is_subscribe><![CDATA[Y]]></is_subscribe>\n<mch_id><![CDATA[1380486802]]></mch_id>\n<nonce_str><![CDATA[ucmfrw627Up1fPNNz6U8bkxUhmUdQ17x]]></nonce_str>\n<openid><![CDATA[oUaMQvwMBomSvpxzSVQTmiJBBuxo]]></openid>\n<out_trade_no><![CDATA[15312128971578318578]]></out_trade_no>\n<result_code><![CDATA[SUCCESS]]></result_code>\n<return_code><![CDATA[SUCCESS]]></return_code>\n<sign><![CDATA[A21152413425868AEB485AFA0897D619]]></sign>\n<time_end><![CDATA[20180710165505]]></time_end>\n<total_fee>1</total_fee>\n<trade_type><![CDATA[JSAPI]]></trade_type>\n<transaction_id><![CDATA[4200000114201807109989693848]]></transaction_id>\n</xml>'
        dict_data = wxClass.to_dict(data)
        if not wxClass.check(dict_data):
            self.AppLogging.warning('签名验证失败')
            self.write(wxClass.reply("FAIL", False))
        else:
            # # 处理业务逻辑
            # 通知成功
            print(dict_data)
            out_trade_no=dict_data['out_trade_no']
            #检查是否支付
            # dbFind=self.db.query(online_order).filter(online_order.orderNum==out_trade_no).first()
            dbFind = None
            if hasattr(dbFind,'id'):
                pass
            else:
                self.AppLogging.warning('支付成功订单不存在%s'%data)
            self.write(wxClass.reply("OK", True))


class wxjsapiSuccess(CommonWeixinPay):
    '''
    jsapi成功后跳转
    '''

    def get(self):
        self.render('WeixinPay/success.html')


class wxjsapiError(CommonWeixinPay):
    '''
    jsapi失败后跳转
    '''

    def get(self):
        ermsg = self.get_argument('errmsg', '')
        ermsg = '支付失败'
        self.render('WeixinPay/error.html', ermsg=ermsg)
