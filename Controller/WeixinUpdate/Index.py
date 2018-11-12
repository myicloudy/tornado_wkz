# coding:utf-8
from tornado.queues import Queue
import hashlib
import asyncio
from datetime import datetime
from asyncio import Queue
import aiohttp
import time
from Controller.WeixinUpdate.CommonWeixinUpdate import CommonWeixinUpdate
from Model.shopmall_main_db.online_business_weixin_config import online_business_weixin_config
from Model.shopmall_main_db.online_provinces import online_provinces
from Model.shopmall_main_db.online_cities import online_cities
from Model.shopmall_main_db.online_userinfo_weixin import online_userinfo_weixin
import logging.config
# 加载前面的标准配置
from Config.logging import LOGGING
import time
logging.config.dictConfig(LOGGING)

import aiohttp
import asyncio


# 开启携程数量
workCounts = 50
q = Queue(maxsize=workCounts)

# 公用变量
clearCount = 0
insertCount = 0
updateCount = 0
ii = 0  # 计数器
bOpenidList = {}  # 数据库中openidlist

sleep_interval = 0  # 携程睡眠时间
max_tasks = workCounts  # 最大携程数

weixinOpenidList = {}

# import pydf
# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class Index(CommonWeixinUpdate):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return: 
        '''
        pass

    async def get(self):
        stime=time.time()
        # 正式服务器通过获取值来处理
        # bidstr = '15_0995b62416f21f59ba6baba7b203016b'
        bidstr = self.get_argument("bid",None)
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
        # 结束

        # bid=38
        # 初始化一些参数
        global insertCount
        insertCount = 0
        global updateCount
        updateCount = 0
        clearCount = 0
        global bOpenidList
        bOpenidList = {}

        # weixinOpenidList = {}
        stime = time.time()
        logger = self.AppLogging
        # bid = 15
        # bid = 38  # 万科数据大概2W
        # 取出微信的微信数据
        weixindb = online_business_weixin_config
        query = self.db.query(weixindb)
        s = query.filter(weixindb.bid == bid).first()
        weixin_AppID = s.weixin_AppID
        weixin_AppSecret = s.weixin_AppSecret

        # # 导入微信类
        # from Tool.Weixin import class_weixin_api
        # wxClass = class_weixin_api(bid, weixin_AppID, weixin_AppSecret)
        # # 获取一下token
        # wxClass.getCredentialAccessToken()
        # print('------token----', wxClass.AccessToken)

        # 公用token
        # 导入微信类
        from Tool.WeixinUpdate.Weixin import class_weixin_api
        wxClass = class_weixin_api(bid, weixin_AppID, weixin_AppSecret)
        # 正式服务器
        tk = wxClass.getCredentialAccessToken()
        print(tk)

        ##############---取微信的数据--start----################
        nextOpenid = 'start'
        openidList = []
        logger.info('获取所有opendid开始')
        while (nextOpenid):
            if nextOpenid == 'start':
                nextOpenid = ''
            weixinAllOpenid = wxClass.getAllOpenidList(nextOpenid)
            nextOpenid = weixinAllOpenid['next_openid']
            if not nextOpenid:
                break
            openidList.extend(weixinAllOpenid['data']['openid'])  # 追加数据

        logger.info("获取到所有openidlist:%s个" % len(openidList))
        print(openidList)

        # 多线程执行吧微信数据取回来。放到memchache



        ##############---处理微信取回来的数据--start----################
        # 查找省份数据
        proveinList = self.getProvincesList()
        # 查找市数据
        citiesList = self.getCitesList()

        # 查询出用户数据
        bidUserList = self.db.query(online_userinfo_weixin).filter(online_userinfo_weixin.bid == bid).all()

        # 同一个unionid用户隶属一个用户
        unionArr = {}

        # 取消关注用户
        killUserIdList = {}
        for row in bidUserList:
            # global bOpenidList
            bOpenidList[row.openid] = row.id
            unionArr[row.unionid] = row.userId
            # 取消关注用户
            if row.openid not in openidList:
                killUserIdList[row.openid] = row.id
                ##############---处理微信取回来的数据--end----################
        unionArr = {}
        wx = online_userinfo_weixin

        crawler = Crawler(openidList, max_tasks=max_tasks)
        await crawler.run(bid, wxClass, unionArr, proveinList, citiesList, self.db, logger)

        # loop = asyncio.get_event_loop()  # 开启主进程
        # crawler = Crawler(openidList, max_tasks=max_tasks)
        # loop.run_until_complete(
        #     crawler.run(bid, wxClass, unionArr, proveinList, citiesList, self.db, logger))  # 直到进程结束退出
        print(time.time()-stime)

        crawler.close()
        #
        # print(weixinOpenidList)
        # self.write(weixinOpenidList)
        ########## loop.close() #loop不能结束。结束了就只能执行一次

        ##############---取微信的数据--end----################




        # ##############---数据库操作--start----################




        # self.db.commit()
        ##############---数据库操作--end----################
        # print('Done')
        ###############----清除取消关注的人---#############
        intime = int(time.time())
        for openid in killUserIdList:
            dblist = {

                'subscribe': 0,
                'subscribe_time': 0,
                'uptime': intime,

            }
            clearCount += 1
            updateUserId = bOpenidList[openid]
            try:
                rr = self.db.query(wx).filter(wx.id == updateUserId).update(dblist, synchronize_session=False)
                self.db.commit()
            except:
                print('-----db update quxiaoguanzhu error-------')
        ###############----清除取消关注的人---#############

        print(bOpenidList)

        etime = time.time()
        runtime = etime - stime
        self.write(
            '新增了' + str(insertCount) + '人，更新了' + str(updateCount) + '人,' + '清除：' + str(
                clearCount) + '人,总计执行时间：' + str(
                runtime) + '秒')

    # 查找数据并返回id

    def defindName(self, name, vlist={}):
        for v in vlist:
            if name == vlist[v]:
                return v
                break
        return 0

    # 查找省份数据
    def getProvincesList(self):
        dlist = self.db.query(online_provinces)

        ret = {}
        for row in dlist:
            ret[row.id] = row.ProvinceName
        return ret

    # 查找市数据
    def getCitesList(self):
        dlist = self.db.query(online_cities)
        ret = {}
        for row in dlist:
            ret[row.id] = row.CityName
        return ret

class Crawler(Index):
    def __init__(self, openidList, max_tries=3, max_tasks=10, _loop=None):
        self.loop = _loop or asyncio.get_event_loop()  # 事件循环
        self.max_tries = max_tries  # 出错重试次数
        self.max_tasks = max_tasks  # 并发任务数
        self.urls_queue = Queue(loop=self.loop)  # 地址队列
        self.ClientSession = aiohttp.ClientSession(loop=self.loop)  # aiohttp的session，get地址数据对象
        for openid in openidList:  # 将所有连接put到队列中
            self.urls_queue.put_nowait(openid)
        self.started_at = datetime.now()  # 开始计时
        self.end_at = None

    def close(self):  # 关闭aiohttp的Session对象
        self.ClientSession.close()

    async def handle(self, openid, bid, wxClass, unionArr, proveinList, citiesList, self_db, logger):
        tries = 0
        while tries < self.max_tries:  # 取不到数据会重试3次
            try:
                url = "https://api.weixin.qq.com/cgi-bin/user/info?access_token=" + wxClass.AccessToken + "&openid=" + openid + "&lang=zh_CN"
                # with aiohttp.Timeout(2):
                response = await self.ClientSession.get(
                    url, allow_redirects=False)  # 不禁用重定向的取数据
                jsonArr = await response.json()  # 异步接收返回数据
                if 'errcode' not in jsonArr:
                    break

            except aiohttp.ClientError:
                # await response.release()  # 异步释放资源
                # break
                pass
            # time.sleep(2)
            tries += 1
        try:
            # text = await response.text()#异步接收返回数据
            print('------tries---------:%d' % tries)
            print(jsonArr)
            if 'errcode' in jsonArr:
                self.AppLogging.warning("get user infois error:%s", jsonArr['errcode'])
            else:
                # weixinOpenidList[openid] = jsonArr
                self.doDBwork(openid, bid, jsonArr, unionArr, proveinList, citiesList, self_db, logger)
        finally:
            await response.release()  # 异步释放资源

    async def work(self, bid, wxClass, unionArr, proveinList, citiesList, self_db, logger):
        try:
            while True:
                openid = await self.urls_queue.get()  # 队列中取openid
                await self.handle(openid, bid, wxClass, unionArr, proveinList, citiesList, self_db,
                                  logger)  # 子方法去取数据
                time.sleep(sleep_interval)  # 线程睡眠
                self.urls_queue.task_done()  # 没有任务后结束
        except asyncio.CancelledError:
            pass

    async def run(self, bid, wxClass, unionArr, proveinList, citiesList, self_db, logger):
        # 开启多个工作携程来执行
        workers = [asyncio.Task(self.work(bid, wxClass, unionArr, proveinList, citiesList, self_db, logger),
                                loop=self.loop)
                   for _ in range(self.max_tasks)]
        self.started_at = datetime.now()  # 程序开始时间
        await self.urls_queue.join()  # 连接join到队列中
        self.end_at = datetime.now()  # 结束时间
        for w in workers:
            w.cancel()  # 释放携程

    def doDBwork(self, wxOpenid, bid, tmplist, unionArr, proveinList, citiesList, self_db, logger):
        wx = online_userinfo_weixin
        dbtype, vlist = self.dowork(tmplist, unionArr, proveinList, citiesList)
        print('---------vvvvvvv----------', dbtype)
        print(vlist)
        if dbtype == 1:  # 插入

            dblist = {
                'bid': bid,
                'openid': vlist['openid'],
                'unionid': vlist['unionid'],
                'groupIds': vlist['groupIds'],
                'sex': vlist['sex'],
                'nickname': vlist['nickname'],
                'remark': vlist['remark'],
                'subscribe': vlist['subscribe'],
                'subscribe_time': vlist['subscribe_time'],
                'thumb': vlist['headimgurl'],
                'pid': vlist['pid'],
                'cid': vlist['cid'],
                'uptime': vlist['uptime'],
                'intime': vlist['intime'],
                'indate': vlist['indate'],
            }

            if 'userId' in vlist:
                dblist['userId'] = vlist['userId']
            global insertCount
            insertCount += 1
            print(dblist)
            # raise
            try:
                rr = self_db.execute(wx.__table__.insert(), dblist)
                self_db.commit()
                logger.info('bid=%s,insetr to db %s', bid, dblist)
            except:
                print('----db insert error-----')

        elif dbtype == 2:  # 更新
            dblist = {
                'unionid': vlist['unionid'],
                'sex': vlist['sex'],
                'nickname': vlist['nickname'],
                'remark': vlist['remark'],
                'subscribe': vlist['subscribe'],
                'subscribe_time': vlist['subscribe_time'],
                'thumb': vlist['headimgurl'],
                'pid': vlist['pid'],
                'cid': vlist['cid'],
                'uptime': vlist['uptime'],
                'groupIds': vlist['groupIds'],

            }
            if 'userId' in vlist:
                dblist['userId'] = vlist['userId']
            global updateCount
            updateCount += 1
            try:
                updateUserId = bOpenidList[wxOpenid]
                rr = self_db.query(wx).filter(wx.id == updateUserId).update(dblist, synchronize_session=False)
                self_db.commit()
                logger.info('bid=%s,update to db %s', bid, dblist)
            except:
                print('-----db update error-------')

            # global bOpenidList
            bOpenidList.pop(wxOpenid)
            # self_db.commit()
        else:
            pass

    def dowork(self, tmplist, unionArr, proveinList, citiesList):
        dbtype = 1  # 1插入2更新3取消关注
        indate = int(time.mktime(time.strptime(time.strftime('%Y-%m-%d', time.localtime()), '%Y-%m-%d')))
        intime = int(time.time())
        if not tmplist:
            return None, None
        if not isinstance(tmplist, (dict)):
            return None, None
        # subscribe
        if not 'openid' in tmplist:
            return None, None
        openid = tmplist['openid']
        tmplist['intime'] = intime
        tmplist['uptime'] = intime
        tmplist['indate'] = indate
        # nickname
        if 'nickname' not in tmplist:
            tmplist['nickname'] = ''
        else:
            pass
            # if isinstance(tmplist['nickname'],bytes):
            #     pass
            # else:
            #     tmplist['nickname']=tmplist['nickname'].encode('unicode-escape')
        # headimgurl
        if 'headimgurl' not in tmplist:
            tmplist['headimgurl'] = ''
        # subscribe_time
        if 'subscribe_time' not in tmplist:
            tmplist['subscribe_time'] = 0

        # remark
        if 'remark' not in tmplist:
            tmplist['remark'] = ''
        # 省分id
        if 'province' in tmplist:
            tmplist['pid'] = self.defindName(tmplist['province'], proveinList)
        else:
            tmplist['pid'] = 0
        # 查找市
        if 'city' in tmplist:
            tmplist['cid'] = self.defindName(tmplist['city'], citiesList)
        else:
            tmplist['cid'] = 0
        # 性别
        if 'sex' not in tmplist:
            tmplist['sex'] = 0
        # subscribe
        if 'subscribe' not in tmplist:
            tmplist['subscribe'] = 0
        # userId
        if 'unionid' in tmplist:
            findunionid = tmplist['unionid']
            if findunionid in unionArr:
                userId = unionArr[findunionid]
                if userId:
                    tmplist['userId'] = userId
        else:
            tmplist['unionid'] = ''
        # 用户分组
        if (tmplist['subscribe'] == 0):  # 取消关注清空一些数据
            tmplist['groupId0'] = 0
            tmplist['groupId1'] = 0
            tmplist['groupId2'] = 0
            tmplist['groupIds'] = ''
            tmplist['subscribe'] = 0
            tmplist['subscribe_time'] = 0
            dbtype = 3  # 取消关注
        else:
            h = ''
            for ts in tmplist['tagid_list']:
                # print(ts)
                h += str(ts) + ','
            tmplist['groupIds'] = h[:-1]

        # 存在就更新
        if openid in bOpenidList:
            dbtype = 2
        else:
            dbtype = 1
        return dbtype, tmplist

