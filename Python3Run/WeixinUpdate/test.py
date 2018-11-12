# coding:utf-8
# @Explain : 说明
# @Time    : 2018/8/16 下午4:11
# @Author  : gg
# @FileName: test.py

#测试专用

import sys

sys.path.append('../../')
bid=15
from Tool.Sqlalchemy.shopmall_main_db import self_db
from Tool.Sqlalchemy.base_model import base_mode
from Tool.WeixinUpdate.Weixin import class_weixin_api
business_weixin_config = base_mode('online_business_weixin_config')
s = self_db.query(business_weixin_config).filter(business_weixin_config.id==bid).first()

weixin_AppID = s.weixin_AppID
weixin_AppSecret = s.weixin_AppSecret

# 公用token
# 导入微信类

wxClass = class_weixin_api(bid, weixin_AppID, weixin_AppSecret)
tk = wxClass.getCredentialAccessToken()
print(wxClass.AccessToken)
print(','.join([]))