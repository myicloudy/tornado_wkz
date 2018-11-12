#!/usr/bin/env python
# -*- coding: utf-8 -*-

from urllib import parse
from urllib.parse import quote, unquote, urlencode
import json
# import logging.config
# from Config.logging import LOGGING
# logging.config.dictConfig(LOGGING)

import logging
import traceback

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)s %(message)s',
    filemode='a',)
logger = logging.getLogger('')

from Controller.AliPay.CommonAliPay import CommonAliPay
from alipay.aop.api.AlipayClientConfig import AlipayClientConfig                                                        # 客户端配置类
from alipay.aop.api.DefaultAlipayClient import DefaultAlipayClient                                                      # 默认客户端类
from alipay.aop.api.domain.AlipayTradeWapPayModel import AlipayTradeWapPayModel                                         # 网站支付数据模型类
from alipay.aop.api.request.AlipayTradeWapPayRequest import AlipayTradeWapPayRequest                                # 网站支付响应类
from alipay.aop.api.domain.AlipayTradeQueryModel import AlipayTradeQueryModel                                           # 网站支付查询数据模型类
from alipay.aop.api.request.AlipayTradeQueryRequest import AlipayTradeQueryRequest                                      # 网站支付查询请求类
from alipay.aop.api.response.AlipayTradeQueryResponse import AlipayTradeQueryResponse                                   # 网站支付查询响应类
from alipay.aop.api.domain.AlipayTradeRefundModel import AlipayTradeRefundModel                                         # 网站支付退款数据模型类
from alipay.aop.api.request.AlipayTradeRefundRequest import AlipayTradeRefundRequest                                    # 网站支付退款请求类
from alipay.aop.api.response.AlipayTradeRefundResponse import AlipayTradeRefundResponse                                 # 网站支付退款响应类

# 引入常用类
from Tool.Common import Common
# 引入shopmall_main_db库
from Model.shopmall_main_db.base_model import base_mode

# 交易支付
class Pay(CommonAliPay):

    #关闭xsrf 临时调试 产线放开防攻击
    def check_xsrf_cookie(self):
        pass

    def post(self):

        bid = self.get_argument('bid')
        payConfig = self.DefaultValues[bid]

        # 设置配置信息
        alipay_client_config = AlipayClientConfig()
        alipay_client_config.server_url = payConfig['zfbpay']['url']
        alipay_client_config.app_id = payConfig['zfbpay']['appId']
        alipay_client_config.app_private_key = payConfig['zfbpay']['app_private_key']
        alipay_client_config.alipay_public_key = payConfig['zfbpay']['alipay_public_key']

        # 客户端对象
        client = DefaultAlipayClient(alipay_client_config=alipay_client_config, logger=logger)

        model = AlipayTradeWapPayModel()
        model.productCode = "QUICK_WAP_PAY"
        model.body = "Iphone6 16G"
        model.subject = "苹果手机"
        model.out_trade_no = "20180510ABoeooer014"
        model.timeout_express = "90m"
        model.total_amount = 0.5
        request = AlipayTradeWapPayRequest(biz_model=model)
        request.notify_url = 'http://wox2019.w-lans.com/alipayNotifyUrl.shtml'
        # 得到构造的请求，如果http_method是GET，则是一个带完成请求参数的url，如果http_method是POST，则是一段HTML表单片段
        response = client.page_execute(request, http_method="GET")
        # GET请求获取完成请求的参数
        _result = parse.parse_qs(parse.urlparse(response).query)
        #return self.write(_result)
        info = {
            'errCode': '0',
            'errMsg': '',
            'detail': {
                'app_id': _result['app_id'][0],
                'version': _result['version'][0],
                'format': _result['format'][0],
                'sign_type': _result['sign_type'][0],
                'charset': _result['charset'][0],
                'method': _result['method'][0],
                'timestamp': _result['timestamp'][0],
                'sign': _result['sign'][0],
                'out_trade_no': eval(_result['biz_content'][0])['out_trade_no'],
                'subject': eval(_result['biz_content'][0])['subject'],
                'body': eval(_result['biz_content'][0])['body'],
                'total_amount': eval(_result['biz_content'][0])['total_amount'],
                'biz_content': eval(_result['biz_content'][0]),
                'notify_url': _result['notify_url'][0],
                #'return_url': _result['return_url'][0],
            }
        }
        result = {
            'result': info
        }
        #self.write(json.dumps(result, ensure_ascii=False))
        #self.render('AliPay/request.html', result=info['detail'])

# 交易成功回调通知
class PayNotifyUrl(CommonAliPay):

    #关闭xsrf 临时调试 产线放开防攻击
    def check_xsrf_cookie(self):
        pass

    def post(self):
        sync_result = {
            'bid': 1,
            'body': 'fsfsfsfs',
            'gmt_payment': '2018-06-25 18:23:42',
            'gmt_refund': '2018-07-06 15:11:46.462',
            'out_trade_no': '152992219042431011',
            'trade_no': 'qeqeqeqe',
            'total_amount': '9.60',
            'refund_fee': '9.60',
           ' trade_no': '2018062521001004680536986176'
        }
        common = Common()                             # 实例化常用类对象
        #sync_result = json.loads(self.request.body)   # 获取回调全部订单参数
        bid = sync_result['bid']                      # 商圈ID
        body = sync_result['body']                    # 商品描述
        out_trade_no = sync_result['out_trade_no']    # 商户订单号
        trade_no = sync_result['trade_no']            # 支付宝交易号
        total_amount = sync_result['total_amount']    # 订单金额 单位元
        refund_fee = sync_result['refund_fee']        # 总退款金额 单位元

        '''
          订单信息插入mysql数据库
        '''
        online_alipay_order_list = base_mode('online_alipay_order_list')    #直接使用引入库的表
        alipay_order_list = online_alipay_order_list(bid = bid,
                                                     body = body,
                                                     out_trade_no= out_trade_no,
                                                     trade_no = trade_no,
                                                     total_amount = total_amount * 100,
                                                     refund_fee = refund_fee * 100,
                                                     tradeStatus = 1, #交易成功
                                                     intime = str(common.current_stamp())
                                                     )
        try:
            self.db.merge(alipay_order_list)  # 类似于add添加数据，但是主键添加存在则修改信息  * db指的是shopmall_main_db库
            self.db.commit()
            return self.write('success')
        except:
            self.AppLogging.warning("支付订单信息写入数据库错误%s" % sync_result)

# 交易状态查询
class Query(CommonAliPay):

    def check_xsrf_cookie(self):
        pass

    def post(self):

        bid = self.get_argument('bid')
        out_trade_no = self.get_argument('out_trade_no')
        payConfig = self.DefaultValues[bid]

        # 设置配置信息
        alipay_client_config = AlipayClientConfig()
        alipay_client_config.server_url = payConfig['zfbpay']['url']
        alipay_client_config.app_id = payConfig['zfbpay']['appId']
        alipay_client_config.app_private_key = payConfig['zfbpay']['app_private_key']
        alipay_client_config.alipay_public_key = payConfig['zfbpay']['alipay_public_key']

        # 客户端对象
        client = DefaultAlipayClient(alipay_client_config=alipay_client_config, logger=logger)

        # 构造请求对象
        model = AlipayTradeQueryModel()
        model.out_trade_no = out_trade_no
        request = AlipayTradeQueryRequest(biz_model=model)
        response_content = None
        try:
            response_content = client.execute(request)
        except Exception as e:
            print('1-' + traceback.format_exc())
        if not response_content:
            print( "2-failed execute")
        else:
            response = AlipayTradeQueryResponse()
            # 解析响应结果
            response.parse_response_content(response_content)
            print(response.body)
            if response.is_success():
                # 业务成功，则通过respnse属性获取需要的值
                print("业务成功- get response trade_no:" + response.trade_no)
                info = {
                    'errCode': '0',
                    'errMsg':  '',
                    'detail': {
                        'trade_no': response.trade_no
                    }
                }
                result = {
                    'result': info
                }
                self.write(json.dumps(result, ensure_ascii=False))
            else:
                # 业务失败
                print('业务失败-' + response.code + "," + response.msg + "," + response.sub_code + "," + response.sub_msg)
                info = {
                    'errCode': response.code,
                    'errMsg':  response.sub_msg
                }
                result = {
                    'result': info
                }
                self.write(json.dumps(result, ensure_ascii=False))

# 交易退款
class Refund(CommonAliPay):

    #关闭xsrf 临时调试 产线放开防攻击
    def check_xsrf_cookie(self):
        pass

    def post(self):

        common = Common()
        print(common.re_phone('13631255697'))

        bid = self.get_argument('bid')
        payConfig = self.DefaultValues[bid]
        out_trade_no = self.get_argument('out_trade_no')
        refund_amount = self.get_argument('refund_amount')
        # if self.get_argument('out_request_no'):
        #   _request_no = self.get_argument('out_request_no')
        #self.write(str(self.get_argument('out_request_no')))
        out_request_no = common.random_str(16)

        # 设置配置信息
        alipay_client_config = AlipayClientConfig()
        alipay_client_config.server_url = payConfig['zfbpay']['url']
        alipay_client_config.app_id = payConfig['zfbpay']['appId']
        alipay_client_config.app_private_key = payConfig['zfbpay']['app_private_key']
        alipay_client_config.alipay_public_key = payConfig['zfbpay']['alipay_public_key']

        # 客户端对象
        client = DefaultAlipayClient(alipay_client_config=alipay_client_config, logger=logger)

        # 直接使用引入库的表
        online_alipay_order_list = base_mode('online_alipay_order_list')

        # 获取该笔订单金额
        order_info = self.db.query(online_alipay_order_list).filter(online_alipay_order_list.bid == bid,
                                                                      online_alipay_order_list.out_trade_no == out_trade_no).scalar()
        # 退款金额大于该笔订单金额 退款失败
        if  int(refund_amount) > int(order_info.total_amount):
            info = {
                'errCode': -1,
                'errMsg': '退款失败，退款金额大于该订单金额'
            }
            result = {
                'result': info
            }
            return self.write(json.dumps(result, ensure_ascii=False))

        # 构造请求对象
        model = AlipayTradeRefundModel()
        model.out_trade_no = out_trade_no
        model.refund_amount = refund_amount
        model.out_request_no = out_request_no
        request = AlipayTradeRefundRequest(biz_model=model)
        response_content = None
        try:
            response_content = client.execute(request)
        except Exception as e:
            print('1-' + traceback.format_exc())
        if not response_content:
            print("2-failed execute")
        else:
            response = AlipayTradeRefundResponse()
            # 解析响应结果
            response.parse_response_content(response_content)
            if response.is_success():
                '''
                   退款成功更新订单信息
                '''
                if not order_info.refundList:
                    refund_list = []
                else:
                    refund_list = eval(order_info.refundList)
                refundInfo = {}
                refundInfo['refund_no'] = out_request_no
                refundInfo['refund_fee'] = int(refund_amount)
                refundInfo['refund_status'] = 1 #退款成功
                refundInfo['refund_date'] = common.current_stamp()
                refund_list.append(refundInfo)
                self.db.query(online_alipay_order_list).filter(
                    online_alipay_order_list.bid == bid,
                    online_alipay_order_list.out_trade_no == out_trade_no).update(
                    {
                        'totalFeeLeft': int(order_info.totalFeeLeft) - int(refund_amount),
                        'refundList': str(refund_list)
                    })
                self.db.commit()
                # 业务成功返回
                info = {
                    'errCode': '0',
                    'errMsg': '',
                    'detail': {
                        'refund_fee': response.refund_fee
                    }
                }
                result = {
                    'result': info
                }
                return self.write(json.dumps(result, ensure_ascii=False))
            else:
                '''
                   退款失败更新订单信息
                '''
                if not order_info.refundList:
                    refund_list = []
                else:
                    refund_list = eval(order_info.refundList)
                refundInfo = {}
                refundInfo['refund_no'] = out_request_no
                refundInfo['refund_fee'] = int(refund_amount)
                refundInfo['refund_status'] = 2  #退款失败
                refundInfo['refund_date'] = common.current_stamp()
                refundInfo['refund_errmsg'] = response.sub_msg
                refund_list.append(refundInfo)
                self.db.query(online_alipay_order_list).filter(
                    online_alipay_order_list.bid == bid,
                    online_alipay_order_list.out_trade_no == out_trade_no).update(
                    {
                       'refundList': str(refund_list)
                    })
                self.db.commit()
                # 业务失败
                info = {
                    'errCode': response.code,
                    'errMsg': response.sub_msg
                }
                result = {
                    'result': info
                }
                return self.write(json.dumps(result, ensure_ascii=False))

# 支付发起页展示
class alipayRequest(CommonAliPay):
    def get(self):
        info = {
            'app_id': 'eqeqeqeq',
            'timestamp': 123142412,
            'sign': 'fdfafafa',
            'out_trade_no': 'riiiibb',
            'subject': '苹果手机',
            'body': 'IPHONE X',
            'total_amount': 100,
            'biz_content': {'a':'1'},
            'notify_url': 'ada',
            'return_url': 'ff'
        }
        self.render('AliPay/request.html', result=info)

# 支付成功页展示
class PaySuccess(CommonAliPay):

    def get(self):
        self.render('AliPay/success.html')

# 支付失败页展示
class PayFail(CommonAliPay):

    def get(self):
        ermsg = self.get_argument('errmsg', '')
        ermsg = '支付失败'
        self.render('AliPay/error.html', ermsg=ermsg)