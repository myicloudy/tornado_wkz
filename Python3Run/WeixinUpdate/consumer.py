# coding:utf-8
# @Explain : 说明
# @Time    : 2018/4/26 上午11:02
# @Author  : gg
# @FileName: consumer.py
# 运行的时候要cd到这个目录
import sys

sys.path.append('../../')
from DefaultValue import DefaultValues
from Tool.Sqlalchemy.shopmall_main_db import self_db
from Tool.Sqlalchemy.base_model import base_mode

from Tool.WeixinUpdate.Weixin import class_weixin_api

# online_analysis_list = base_mode('online_analysis_list')
# dblist = self_db.query(online_analysis_list.title).all()
# print(dblist)
# print(self_db)

from kafka import KafkaConsumer
import time
import json


# To consume latest messages and auto-commit offsets
# ,group_id='group1'


# tmp = {'bid': '15', 'o5Hb6wrPm7uUFfGSSXEd7a8DiYo8': 3392366, 'o5Hb6wtWAEipeEX64P8rKwI9RSPQ': 3392367,
#        'o5Hb6wnqZFdxZr4TEmdyUwcFAoVc': 3392394, 'o5Hb6wqnXYszLZMT6I8a9IJyUl70': 0,
#        }


def get_subscribe_scene_type(value):
    subscribe_scene_type_config = DefaultValues['subscribe_scene_type_config']
    for k, v in enumerate(subscribe_scene_type_config):
        if v['valcode'] == value:
            return k
    return 0


def weixin_user_info_batchget(mvalue):
    insertCount = 0
    updateCount = 0
    clearCount = 0
    clearCountNone = 0
    errCount=0
    bid = mvalue['bid']
    userinfo_weixin = base_mode('online_userinfo_weixin')
    business_weixin_config = base_mode('online_business_weixin_config')
    s = self_db.query(business_weixin_config).filter(business_weixin_config.bid == bid).first()
    weixin_AppID = s.weixin_AppID
    weixin_AppSecret = s.weixin_AppSecret
    # 公用token
    # 导入微信类
    wxClass = class_weixin_api(bid, weixin_AppID, weixin_AppSecret)
    tk = wxClass.getCredentialAccessToken()
    post_data = weixin_batchget_post_date(mvalue)
    if post_data is None:
        print('post数据错误，或者没有数据')
        return
    # print(post_data)
    res = wxClass.user_info_batchget(post_data)
    # print(res)
    if res is None:
        print('微信接口无数据')
        return

    user_info_list = res['user_info_list']

    for vlist in user_info_list:
        subscribe = vlist['subscribe']
        openid = vlist['openid']
        if subscribe == 0 or subscribe == '0':  # 未关注就删除
            dblist = {
                'subscribe': 0,
                'subscribe_time': 0,
                'uptime': int(time.time()),
            }
            if openid in mvalue:  # 如果在数据库内 update
                updateUserId = mvalue[openid]
                try:
                    rr = self_db.query(userinfo_weixin).filter(userinfo_weixin.id == updateUserId).update(dblist,
                                                                                                      synchronize_session=False)
                    self_db.commit()
                    clearCount = clearCount + 1
                except:
                    errCount=errCount+1

            else:
                clearCountNone = clearCountNone + 1
        else:
            tagid_list = vlist.get('tagid_list', [])

            if tagid_list == '[]':
                tagid_list_out = ''
            else:
                tagid_list_out = ",".join(str(x) for x in tagid_list)
            isIn = mvalue.get(openid, 0)
            if isIn != 0:  # 如果在数据库内 update
                dblist = {
                    'unionid': vlist.get('unionid', ''),
                    'sex': vlist.get('sex', 0),
                    'nickname': vlist.get('nickname', ''),
                    'remark': vlist.get('remark', ''),
                    'subscribe': vlist.get('subscribe', 0),
                    'subscribe_time': vlist.get('subscribe_time', 0),
                    'thumb': vlist.get('headimgurl', ''),
                    'groupIds': tagid_list_out,
                    'country': vlist.get('country', ''),
                    'province': vlist.get('province', ''),
                    'city': vlist.get('city', ''),
                    'subscribe_scene_type': get_subscribe_scene_type(vlist.get('subscribe_scene', '')),
                    'uptime': int(time.time()),
                }
                updateUserId = mvalue[openid]

                try:
                    rr = self_db.query(userinfo_weixin).filter(userinfo_weixin.id == updateUserId).update(dblist,
                                                                                                          synchronize_session=False)
                    self_db.commit()
                    updateCount = updateCount + 1
                except:
                    errCount=errCount+1


            else:
                dblist = {
                    'bid': bid,
                    'openid': openid,
                    'unionid': vlist.get('unionid', ''),
                    'sex': vlist.get('sex', 0),
                    'nickname': vlist.get('nickname', ''),
                    'remark': vlist.get('remark', ''),
                    'subscribe': vlist.get('subscribe', 0),
                    'subscribe_time': vlist.get('subscribe_time', 0),
                    'thumb': vlist.get('headimgurl', ''),
                    'groupIds': tagid_list_out,
                    'country': vlist.get('country', ''),
                    'province': vlist.get('province', ''),
                    'city': vlist.get('city', ''),
                    'subscribe_scene_type': get_subscribe_scene_type(vlist.get('subscribe_scene', '')),
                    'uptime': int(time.time()),
                }
                try:
                    rr = self_db.execute(userinfo_weixin.__table__.insert(), dblist)
                    self_db.commit()
                    insertCount = insertCount + 1
                except:
                    errCount=errCount+1


    print('总计：' + str(len(mvalue) - 1))
    print('增加：' + str(insertCount))
    print('更新：' + str(updateCount))
    print('清除：' + str(clearCount))
    print('错误：' + str(errCount))
    print('未知：' + str(clearCountNone))


def weixin_batchget_post_date(mvalue):
    '''
    数据组装微信接口的数据体
    :param mvalue: 
    :return: 
    '''
    alljson = []
    for v in mvalue:
        if v == 'bid':
            continue
        tmp = {}
        tmp['openid'] = v
        tmp['lang'] = 'zh_CN'
        alljson.append(tmp)
    if not alljson:
        return None
    return {'user_list': alljson}


# print(tmp)


consumer = KafkaConsumer(DefaultValues['WeixinUpdate_topic'],
                         bootstrap_servers=DefaultValues['WeixinUpdate_bootstrap_servers'],
                         group_id=DefaultValues['WeixinUpdate_group_id'],
                         value_deserializer=lambda m: json.loads(m.decode('utf-8')))
for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    # print("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
    #                                      message.offset, message.key,
    #                                      message.value))
    weixin_user_info_batchget(message.value)
    # print(dict['bid'])
    consumer.commit()
    # time.sleep(0.1)
