# coding:utf-8
# -*- coding: utf-8 -*-
# @Explain : 微信类
# @Time    : 2017/11/27 上午11:41
# @Author  : gg
# @FileName: Weixin.py
import html
import json
from urllib.parse import unquote

import requests
import tornado.httpclient

from Controller.Control import ControlHandler

from .base import WeixinError


class WeixinLoginError(WeixinError):

    def __init__(self, msg):
        super(WeixinLoginError, self).__init__(msg)

class class_weixin_api(ControlHandler):
    def __init__(self, bid, appId, appSecret):
        self.requestSession=requests.session()
        self.weixin_api_appId = appId
        self.weixin_api_appSecret = appSecret
        self.weixin_api_bid = bid

    def _get(self, url, params):
        resp = self.requestSession.get(url, params=params)
        # print(resp.content.decode("utf-8"))
        data = json.loads(resp.content.decode("utf-8"))
        if 'errcode' in data:
            msg = "%(errcode)d %(errmsg)s" % data
            raise WeixinLoginError(msg)
        return data

    def access_token(self, code):
        """
        获取令牌
        """
        mname = 'access_token' + str(self.weixin_api_bid)
        # self.mrcacheDelete(mname)
        mvalue = self.mrcacheGet(mname)
        if mvalue:
            return mvalue
        else:
            url = "https://api.weixin.qq.com/sns/oauth2/access_token"
            args = dict()
            args.setdefault("appid", self.weixin_api_appId)
            args.setdefault("secret", self.weixin_api_appSecret)
            args.setdefault("code", code)
            args.setdefault("grant_type", "authorization_code")
            res = self._get(url, args)
            self.mrcacheSet(mname, res, 7100)
            return res

    def snsUserinfo(self, access_token, openid):
        """
        获取用户信息

        :param access_token: 令牌
        :param openid: 用户id，每个应用内唯一
        """
        url = "https://api.weixin.qq.com/sns/userinfo"
        args = dict()
        args.setdefault("access_token", access_token)
        args.setdefault("openid", openid)
        args.setdefault("lang", "zh_CN")

        return self._get(url, args)

    # 获取普通access_token
    # author     : gg
    # create date    : 2017-12-15
    def getCredentialAccessToken(self, refresh=0):
        mname='getCredentialAccessToken_'+str(self.weixin_api_bid)
        # self.mrcacheDelete(mname)
        mvalue=self.mrcacheGet(mname)
        if mvalue:
            return mvalue
        else:
            # 获取token
            url = "https://api.weixin.qq.com/cgi-bin/token?grant_type=client_credential"
            args = dict()
            args.setdefault("appid", self.weixin_api_appId)
            args.setdefault("secret", self.weixin_api_appSecret)
            res= self._get(url, args)
            self.mrcacheSet(mname,res['access_token'],7100)
            return res['access_token']



    def authorize(self, redirect_uri, scope="snsapi_base", state=None):
        """
        生成微信认证地址并且跳转

        :param redirect_uri: 跳转地址
        :param scope: 微信认证方式，有`snsapi_base`跟`snsapi_userinfo`两种
        :param state: 认证成功后会原样带上此字段
        """
        url = "https://open.weixin.qq.com/connect/oauth2/authorize"
        assert scope in ["snsapi_base", "snsapi_userinfo"]
        data = dict()
        data.setdefault("appid", self.weixin_api_appId)
        data.setdefault("redirect_uri", redirect_uri)
        data.setdefault("response_type", "code")
        data.setdefault("scope", scope)
        if state:
            data.setdefault("state", state)
        data = [(k, data[k]) for k in sorted(data.keys()) if data[k]]
        s = "&".join("=".join(kv) for kv in data if kv[1])
        return "{0}?{1}#wechat_redirect".format(url, s)











#--------old
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
            raise
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
            raise
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
            raise
        jsonArr = json.loads(res)
        print (jsonArr)
        if 'errcode' in jsonArr:
            self.AppLogging.error('获取微信openidlist错误%s' , str(jsonArr['errcode']))
            raise SystemError('apiErr code %d' , jsonArr['errcode'])
        return jsonArr





    # 获取普通access_token
    # author     : gg
    # create date    : 2016-11-22
    def getCredentialAccessToken_old(self):
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
            raise
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
            raise
        tokenArr = json.loads(res)
        if tokenArr['result'] == '0':
            self.AppLogging.error(tokenArr['resultNote'])
            raise
        return tokenArr['Detail']['Accessor_Token']

    # 同步取数据
    def http_client(self, url, body=None):
        http = tornado.httpclient.HTTPClient()
        request = tornado.httpclient.HTTPRequest(url, method='POST', body=body)
        res = http.fetch(request)
        return res.body

    # 同步取数据get
    def http_client_get(self, url):
        http = tornado.httpclient.HTTPClient()
        request = tornado.httpclient.HTTPRequest(url)
        res = http.fetch(request)
        return res.body

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
