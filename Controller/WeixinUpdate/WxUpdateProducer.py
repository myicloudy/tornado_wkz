# coding:utf-8

from Controller.Message.CommonMessage import CommonMessage
import tornado
import logging
import logging.config
from Model.shopmall_main_db.base_model import base_mode
# 加载前面的标准配置
from Config.logging import LOGGING
import time

logging.config.dictConfig(LOGGING)
from kafka import KafkaProducer
import json
from Tool.WeixinUpdate.Weixin import class_weixin_api
import hashlib

# import pydf
# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class WxUpdateProducer(CommonMessage):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return: 
        '''
        pass

    def get(self):
        bidstr = self.get_argument("bid", None)
        # bidstr = '15_0995b62416f21f59ba6baba7b203016b'
        if not bidstr:
            self.write('请选择商圈')
            return {}
        bids = bidstr.split('_')
        bids.append(0)
        bids.append(0)
        bid = bids[0]
        mkey = bids[1]
        if not bid or not mkey:
            self.write('参数错误')
            return {}
        md5key = 'requestmmmdddd5555'
        m2 = hashlib.md5()
        strss = md5key + '' + str(bid)
        m2.update(strss.encode('utf-8'))
        thismkey = m2.hexdigest()
        if not thismkey == mkey:
            self.write('校验串错误')
            return {}
        # 取出微信的微信数据
        # bid = '233'
        weixindb = base_mode('online_business_weixin_config')
        query = self.db.query(weixindb)
        s = query.filter(weixindb.bid == bid).first()
        weixin_AppID = s.weixin_AppID
        weixin_AppSecret = s.weixin_AppSecret

        # 公用token
        # 导入微信类

        wxClass = class_weixin_api(bid, weixin_AppID, weixin_AppSecret)
        # 正式服务器
        tk = wxClass.getCredentialAccessToken()
        print(tk)

        ##############---取微信的数据--start----################
        nextOpenid = 'start'
        openidList = []
        self.AppLogging.info('获取所有opendid开始')
        while (nextOpenid):
            if nextOpenid == 'start':
                nextOpenid = ''
            weixinAllOpenid = wxClass.getAllOpenidList(nextOpenid)
            nextOpenid = weixinAllOpenid['next_openid']
            if not nextOpenid:
                break
            openidList.extend(weixinAllOpenid['data']['openid'])  # 追加数据

        self.AppLogging.info("获取到所有openidlist:%s个" % len(openidList))
        # print(openidList)

        # 取出商圈的所有数据
        wx = base_mode('online_userinfo_weixin')
        ulist = self.db.query(wx.openid, wx.id).filter(wx.bid == bid).all()
        db_openlist = {}
        for index in ulist:
            db_openlist[index[0]] = index

        producer = KafkaProducer(bootstrap_servers=self.DefaultValues['WeixinUpdate_bootstrap_servers'],
                                 value_serializer=lambda v: json.dumps(v).encode('utf-8'))

        # bytes(openid, encoding = "utf8")
        clearCount = 0
        qOpenidList = list(set(db_openlist).difference(set(openidList)))
        for qopenid in qOpenidList:
            updateUserId = db_openlist[qopenid][1]
            dblist = {
                'subscribe': 0,
                'subscribe_time': 0,
                'uptime': int(time.time()),
            }
            rr = self.db.query(wx).filter(wx.id == updateUserId).update(dblist, synchronize_session=False)
            self.db.commit()
            clearCount = clearCount + 1
        print("获取到所有openidlist:%s个" % len(openidList))
        print('清除：' + str(clearCount))
        ii = 1
        openidFindList = {}
        for openid in openidList:
            # 吧数据库内数据添加到内部，判断是否有更改，
            openidFindList['bid'] = bid
            if openid in db_openlist:
                openidFindList[openid] = db_openlist[openid][1]
            else:
                openidFindList[openid] = 0
            if ii % 100 == 0:  # 接口可以一次性读取100条数据，那么每100条数据提交kafka
                producer.send(self.DefaultValues['WeixinUpdate_topic'], value=openidFindList)
                openidFindList = {}
            ii = ii + 1
        if openidFindList:
            producer.send(self.DefaultValues['WeixinUpdate_topic'], value=openidFindList)
        producer.flush()
        self.write('已经发送到消息队列处理中。。。。')
        # self.write('ok')
        # producer = KafkaProducer(
        #     bootstrap_servers=['120.25.209.7:9092','120.25.209.7:9093'])  # 此处ip可以是多个['0.0.0.1:9092','0.0.0.2:9092','0.0.0.3:9092' ]
        #
        #
        # for i in range(10):
        #     producer.send('test2', b'message %d' % i)
        #
        # producer.flush()
