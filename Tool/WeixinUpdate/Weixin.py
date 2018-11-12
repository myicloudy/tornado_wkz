# coding:utf-8
# -*- coding: utf-8 -*-
# @Explain : 微信类
# @Time    : 2017/11/27 上午11:41
# @Author  : gg
# @FileName: Weixin.py
from tornado import gen
import tornado.httpclient
import json
from Controller.Control import ControlHandler
import html
from urllib.parse import unquote
import requests


class class_weixin_api(ControlHandler):
    def __init__(self, bid, appId, appSecret):
        self.weixin_api_appId = appId
        self.weixin_api_appSecret = appSecret
        self.weixin_api_bid = bid


    # 发送客服消息
    # author     : gg
    # create date    : 2017-12-29
    def message_custom_send(self,sendContent):
        url = 'https://api.weixin.qq.com/cgi-bin/message/custom/send?access_token=' + self.AccessToken
        print(sendContent)
        data=json.dumps(sendContent)
        end_data = unquote(data)
        end_data = html.unescape(end_data)
        print(url)
        res = self.http_client(url,end_data)
        if res == None:
            self.AppLogging.error('发送客服消息错误')
            return None
        jsonArr = json.loads(res)
        print(jsonArr)
        return jsonArr

    # 发送微信模板消息
    # author     : gg
    # create date    : 2018-01-05
    def message_template_send(self, sendContent):
        url = 'https://api.weixin.qq.com/cgi-bin/message/template/send?access_token=' + self.AccessToken
        print(sendContent)
        data = json.dumps(sendContent)
        end_data = unquote(data)
        end_data = html.unescape(end_data)
        print(url)
        res = self.http_client(url, end_data)
        if res == None:
            self.AppLogging.error('发送微信模板消息错误')
            return None
        jsonArr = json.loads(res)
        print(jsonArr)
        return jsonArr

    # 获取所有openid
    # author     : gg
    # create date    : 2016-11-22
    def getAllOpenidList(self, nextOpenid=''):
        url = 'https://api.weixin.qq.com/cgi-bin/user/get?access_token=' + self.AccessToken + '&next_openid=' + nextOpenid
        print (url)
        res = self.http_client_get(url)
        if res == None:
            self.AppLogging.error('获取微信openidlist错误')
            return None
        jsonArr = json.loads(res)
        print (jsonArr)
        if 'errcode' in jsonArr:
            self.AppLogging.error('获取微信openidlist错误%s' , str(jsonArr['errcode']))
            # raise SystemError('apiErr code %d' , jsonArr['errcode'])
            return None
        return jsonArr

    # 获取普通access_token
    # author     : gg
    # create date    : 2017-12-15
    def getCredentialAccessTokenToWeixin(self, refresh=0):
        # 获取token
        url = "https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential&appid=" + self.weixin_api_appId + "&secret=" + self.weixin_api_appSecret

        res = self.http_client_get(url)
        if res == None:
            self.AppLogging.error('获取微信openidlist错误')
            return None
        jsonArr = json.loads(res)
        # print(jsonArr)
        if 'errcode' in jsonArr:
            self.AppLogging.error('获取微信openidlist错误%s' , str(jsonArr['errcode']))
            # raise SystemError('apiErr code %d' , jsonArr['errcode'])
            return None
        self.AccessToken = jsonArr['access_token']
        return jsonArr['access_token']



    # 获取普通access_token
    # author     : gg
    # create date    : 2016-11-22
    def getCredentialAccessToken(self):
        # 用测试方法调用
        token = self.get_bid_access_token_api(self.weixin_api_bid)
        self.AccessToken = token
        return token

    # 获取用户信息
    def base_userinfo(self, openid=None):
        if openid == None:
            return None
        url = "https://api.weixin.qq.com/cgi-bin/user/info?access_token=" + self.AccessToken + "&openid=" + openid + "&lang=zh_CN";
        res =  self.http_client_get(url)
        jsonArr = json.loads(res)
        print(jsonArr)
        if 'errcode' in jsonArr:
            self.AppLogging.error('获取微信base_userinfo错误%s' , str(jsonArr['errcode']))
            return None
        return jsonArr

    def user_info_batchget(self, openid_list=None):
        '''
        批量获取用户信息，最多100条
        :param openid_list: 
        :return: 
        '''
        api_url='https://api.weixin.qq.com/cgi-bin/user/info/batchget?access_token='+self.AccessToken
        res = self.http_client(api_url, json.dumps(openid_list))

        if res == None:
            self.AppLogging.error('批量获取用户信息错误')
            return None
        try:
            jsonArr = json.loads(res)
        except:
            return None
        if 'errcode' in jsonArr:
            self.AppLogging.error('批量获取用户信息错误错误%s', str(jsonArr['errcode']))
        return jsonArr


    # @Explain : 用测试接口获取token
    # @Time    : 2017/11/27 11.51
    # @Author  : gg
    def get_bid_access_token_api(self, bid):
        api_url = 'http://wox.w-lans.com/Test/Apigg.shtml'
        # api_arr=["cmd": "getWeixinBaseToken"]
        jsonload = {
            "cmd": "getWeixinBaseToken",
            "params": {
                "sign": "shopmall",
                "bid": bid
            }
        }
        res = self.http_client(api_url, json.dumps(jsonload))

        if res == None:
            self.AppLogging.error('返回token错误')
            return None
        tokenArr = json.loads(res)
        if tokenArr['result'] == '0':
            self.AppLogging.error(tokenArr['resultNote'])
            return None
        return tokenArr['Detail']['Accessor_Token']

    # 同步取数据
    def http_client(self, url, body=None):
        resp = requests.session().post(url, data=body)
        return resp.content.decode("utf-8")


    # 同步取数据get
    def http_client_get(self, url):
        resp = requests.session().get(url)
        return resp.content.decode("utf-8")


    # 同步取数据get
    @tornado.web.asynchronous
    @tornado.gen.coroutine
    def http_client_get_async(self, url):
        http_client = tornado.httpclient.AsyncHTTPClient()
        response = yield tornado.gen.Task(http_client.fetch, url)
        return response.body


    def on_fetch_async(self, response):
        self.write(response.body)
        self.finish()
        # return response.body
