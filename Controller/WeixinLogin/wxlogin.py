# coding:utf-8

import logging.config

# 加载前面的标准配置
from Config.logging import LOGGING
from Controller.WeixinLogin.CommonWeixinLogin import CommonWeixinLogin

logging.config.dictConfig(LOGGING)
from Tool.WeixinGG.Weixin import class_weixin_api

# #明加
appId='wx787f04067ec0c38d'
appSecret='e238f6b59fb8c417c278c2b76af5fff7'

#测试
# appId='wx7c6a3db1fe42661b'
# appSecret='8e49d929b60b52f53c04fb46f3b028bd'


import urllib



# import pydf
# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class wxlogin(CommonWeixinLogin):
    def get(self):
        bid = self.DefaultValues['bid']
        sname = 'wxLoginInfo' + str(bid)
        if sname in self.session:
            self.redirect('/index.sthml')
        else:

            # self.write(wxClass.getCredentialAccessToken())
            # self.write('sss')

            wxClass = class_weixin_api(bid, appId=appId, appSecret=appSecret)
            redirect_uri=urllib.parse.quote_plus('http://dayu.mingmore.com/wxAuthorize.shtml?bid='+str(bid))
            redirectUrl=wxClass.authorize(redirect_uri,scope='snsapi_userinfo')
            # self.write(redirectUrl)
            self.redirect(redirectUrl)
            # self.write(redirectUrl)

class wxAuthorize(CommonWeixinLogin):
    '''
    获取用户授权信息
    '''
    def get(self):
        bid = self.DefaultValues['bid']
        wxClass = class_weixin_api(bid, appId=appId, appSecret=appSecret)
        code = self.get_argument('code', '')
        # code='011kRHA71HoBfS1xIdA71GVDA71kRHAC'
        if not code:
            self.writeWebError(500,'code error')
        # self.write(code)
        wxCodeInfo=wxClass.access_token(code)
        # self.write(wxCodeInfo)
        openid=wxCodeInfo['openid']
        # print(openid)
        wxInfo=wxClass.snsUserinfo(wxCodeInfo['access_token'],openid)
        #缓存内有数据，更新数据库
        # self.mrcacheDelete(openid)
        mrget=self.mrcacheGet(openid)
        # self.AppLogging.warning("取到缓存%s" % mrget)
        if mrget:
            self.update_db(wxInfo,mrget['id'])
            wxInfo['id'] = mrget['id']
        else:
            # 缓存会剔除很多次，但是最终以数据库为准
            # findid = self.db.query(online_userinfo_weixin).filter(online_userinfo_weixin.openid == openid).first()
            findid=None
            if hasattr(findid, 'id'):
                self.update_db(wxInfo, findid.id)
                wxInfo['id'] = findid.id
            else:
                inDbId=self.insert_db(wxInfo)
                wxInfo['id']=inDbId
        #获取到用户信息更新缓存内
        self.mrcacheSet(openid, wxInfo)
        #设置session
        sname='wxLoginInfo'+str(bid)
        self.session[sname]=wxInfo
        # self.write(wxInfo)
        self.redirect('/index.shtml')
    def update_db(self,wxInfo,id):
        '''
        更新数据
        :param wxInfo: 
        :return: 
        '''
        params = {}
        params = self.dbtime(params,type='update')
        params['unionid'] = wxInfo['unionid']
        params['nickname'] = wxInfo['nickname']
        params['sex'] = wxInfo['sex']
        params['pid'] = self.defindName(wxInfo['province'], self.getProvincesList())
        params['cid'] = self.defindName(wxInfo['city'], self.getCitesList())
        params['thumb'] = wxInfo['headimgurl']

        try:
            self.db.query(online_userinfo_weixin).filter(online_userinfo_weixin.id == id).update(
                params, synchronize_session=False)
            self.db.commit()
        except:
            self.AppLogging.warning("微信授权更新数据库错误%s" % params)

    def insert_db(self,wxInfo):
        '''
        插入数据库
        :return: 插入后自增id
        '''
        params={}
        params=self.dbtime(params)
        params['openid']=wxInfo['openid']
        params['unionid'] = wxInfo['unionid']
        params['nickname'] = wxInfo['nickname']
        params['sex'] = wxInfo['sex']
        params['pid'] = self.defindName(wxInfo['province'], self.getProvincesList())
        params['cid'] = self.defindName(wxInfo['city'], self.getCitesList())
        params['thumb'] = wxInfo['headimgurl']
        try:
            dbreturn = self.db.execute(online_userinfo_weixin.__table__.insert(), params)
            self.db.commit()
            return dbreturn.lastrowid
        except:
            self.AppLogging.warning("微信授权写入数据库错误%s" % params)