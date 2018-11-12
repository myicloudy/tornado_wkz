# coding:utf-8

from Controller.ContabUrl.CommonContabUrl import CommonContabUrl
import tornado
import logging
import logging.config

import time
import datetime
import pandas as pd
import json
# 加载前面的标准配置
from Config.logging import LOGGING

logging.config.dictConfig(LOGGING)
from Model.shopmall_main_db.base_model import base_mode
from Model.shopmall_report.base_model_report import base_model_report
from Model.shopmall_conduct.base_model_conduct import base_mode_conduct
from datetime import datetime, date, timedelta

# import aiohttp
# import asyncio
# import numpy as np

pd.set_option('display.max_columns', None)  # 显示完整列


# pd.set_option('display.max_rows', None)  # 设置显示完整行数

# import pandas
# @Explain : 定时任务统计商圈数据信息
# @Time    : 2018/08/20 下午2:40
# @Author  : yt
class Index(CommonContabUrl):
    def prepare(self):
        '''
        初始化时载入1.initialize 2.perpare
        :return: 
        '''
        pass

    def getDatetimeToday(self):
        '''
        获取datetime.datetime类型当前日期
        '''
        t = date.today()  # date类型
        dt = datetime.strptime(str(t), '%Y-%m-%d')  # date转str再转datetime
        return dt

    def getDatetimeYesterday(self):
        '''
        获取datetime.datetime类型前一天日期
        '''
        today = self.getDatetimeToday()  # datetime类型当前日期
        yesterday = today + timedelta(days=-1)  # 减去一天
        return yesterday

    def get(self):
        # analysis_list = base_mode('online_analysis_list')
        # dbfind = self.db.query(analysis_list.id, analysis_list.title).filter(analysis_list.id < 100).all()
        # self.write(str(self.db.query(analysis_list.id, analysis_list.title).filter(analysis_list.id < 100)))#打印sql语句
        # self.write( str(dbfind)) # 获取查询的内容信息
        inarr = self.dbtime()
        # 默认执行前一天数据
        ydt = self.getDatetimeYesterday()
        btime = self.get_argument("sDate", int(time.mktime(time.strptime(str(ydt), "%Y-%m-%d %H:%M:%S"))))
        etime = btime

        userinfo = base_mode('online_userinfo')
        user_active = base_model_report('online_user_active')  # 用户活跃表
        a_business_statistics = base_mode('online_a_business_statistics')  # 商圈统计数据行为表
        behavior_list = base_mode_conduct('online_behavior_list')  # 用户行为分析表
        userinfo = base_mode('online_userinfo')  # 会员表

        # 测试
        # sindate = 1467907200
        indate = 1505664000
        bid = 15
        # 获取某个日期及之前的数据
        userinfoList = self.db.query(userinfo.id, userinfo.sid, userinfo.cardLeve, userinfo.age, userinfo.sex,
                                     userinfo.fromId, userinfo.integral, userinfo.wxId, userinfo.sourceFrom).filter(
            userinfo.bid == bid,
            userinfo.indate <= indate).all()
        userinfoList_pd = pd.DataFrame(userinfoList,
                                       columns=['id', 'sid', 'cardLeve', 'age', 'sex', 'fromId', 'integral', 'wxId',
                                                'sourceFrom'])
        # 获取当前日期的数据
        userinfoDayList = self.db.query(userinfo.id, userinfo.sid, userinfo.cardLeve, userinfo.age,
                                        userinfo.sex, userinfo.fromId, userinfo.integral, userinfo.wxId,
                                        userinfo.sourceFrom).filter(
            userinfo.bid == bid, userinfo.indate == indate).all()
        userinfoDayList_pd = pd.DataFrame(userinfoDayList,
                                          columns=['id', 'sid', 'cardLeve', 'age', 'sex', 'fromId', 'integral', 'wxId',
                                                   'sourceFrom'])

        # 商圈活跃数
        todayHyList = self.db_conduct.query(behavior_list.__table__).filter(behavior_list.bid == bid,
                                                                            behavior_list.indate == indate).limit(
            500000).all()
        todayHyList_pd = pd.DataFrame(todayHyList,
                                      columns=['id', 'userType', 'uid', 'bid', 'sid', 'typeId', 'otherId1', 'otherId2',
                                               'otherId3', 'otherId4', 'otherId5', 'ucount', 'card_cate', 'intime',
                                               'indate'])
        # self.electBindingUser(indate, bid, userinfoList_pd, userinfoDayList_pd)
        # 计算商圈天活跃
        todayHyList = self.outDayHuoyue(todayHyList_pd, user_active, indate, bid)
        todayHyUidList = pd.merge(todayHyList[todayHyList['userType'] == 1], userinfoList_pd, how='left', left_on='uid',
                                  right_on='id')  # 获取卡的等级信息
        todayHyWxIdList = pd.merge(todayHyList[todayHyList['userType'] == 2], userinfoList_pd, how='left',
                                   left_on='uid', right_on='wxId')  # 获取卡的等级信息根据wxID
        todayHyUserList_pd = todayHyUidList.append(todayHyWxIdList)
        self.userinfo_cardLeve(todayHyUserList_pd, indate, bid)
        return

        # 取出还没有执行的商圈（每次只执行一个商圈）, 且上次执行时间小于今天的
        dt = datetime.strptime(str(date.today()), '%Y-%m-%d')  # date转str再转datetime
        todayTime = int(time.mktime(time.strptime(str(dt), "%Y-%m-%d %H:%M:%S")))  # 今天0点时间戳
        blist = self.db.query(a_business_statistics.__table__).filter(
            'Behavior=0 and BehaviorTime<' + str(todayTime)).all()
        if len(blist) == 0:  # 都执行完，把所有商圈状态还原为0，隔天再次执行
            self.db.query(a_business_statistics).update({'Behavior': '0', 'uptime': int(time.time())})
            self.db.commit()
            return

        for i in range(btime, etime, 86400):
            indate = i
            for blist_value in blist:  # 循环所有商圈
                bid = blist_value.bid
                self.db.query(a_business_statistics).filter(
                    a_business_statistics.id == blist_value.id).update(
                    {'Behavior': '1', 'uptime': int(time.time())})  # 把商圈状态改为1
                self.db.commit()

                # 获取某个日期及之前的数据
                userinfoList = self.db.query(userinfo.id, userinfo.sid, userinfo.cardLeve, userinfo.age, userinfo.sex,
                                             userinfo.fromId, userinfo.integral, userinfo.wxId,
                                             userinfo.sourceFrom).filter(
                    userinfo.bid == bid,
                    userinfo.indate <= indate).all()
                userinfoList_pd = pd.DataFrame(userinfoList,
                                               columns=['id', 'sid', 'cardLeve', 'age', 'sex', 'fromId', 'integral',
                                                        'wxId', 'sourceFrom'])
                # 获取当前日期的数据
                userinfoDayList = self.db.query(userinfo.id, userinfo.sid, userinfo.cardLeve, userinfo.age,
                                                userinfo.sex, userinfo.fromId, userinfo.integral, userinfo.wxId,
                                                userinfo.sourceFrom).filter(
                    userinfo.bid == bid, userinfo.indate == indate).all()
                userinfoDayList_pd = pd.DataFrame(userinfoDayList,
                                                  columns=['id', 'sid', 'cardLeve', 'age', 'sex', 'fromId', 'integral',
                                                           'wxId', 'sourceFrom'])

                self.get_user_distribution(indate, bid, 1, userinfoList_pd)  # 当前日期及之前用户分布统计

                self.get_user_distribution(indate, bid, 2, userinfoDayList_pd)  # 当前日期会员增长分布统计

                self.integral_user_distribution(indate, bid)  # 万菱汇商圈积分活跃用户分布统计

                self.integralAddUse_SumCount(indate, bid, userinfoList_pd)  # 计算积分-积分增加额度和笔数-积分使用额度和笔数

                self.integral_cardLeve(indate, bid, userinfoList_pd)  # 卡等级积分统计

                self.integralShop(indate, bid)  # 积分商城

                self.messageShow(indate, bid)  # 消息发送次数

                self.autoIntegral(indate, bid)  # 扫码积分，小票

                self.electBindingUser(indate, bid, userinfoList_pd, userinfoDayList_pd)  # 会员数:包括电子会员和绑定的卡会员

                # 商圈活跃数
                todayHyList = self.db_conduct.query(behavior_list.__table__).filter(behavior_list.bid == bid,
                                                                                    behavior_list.indate == indate).limit(
                    500000).all()
                todayHyList_pd = pd.DataFrame(todayHyList,
                                              columns=['id', 'userType', 'uid', 'bid', 'sid', 'typeId', 'otherId1',
                                                       'otherId2',
                                                       'otherId3', 'otherId4', 'otherId5', 'ucount', 'card_cate',
                                                       'intime',
                                                       'indate'])

                # 计算商圈天活跃
                todayHyList = self.outDayHuoyue(todayHyList_pd, user_active, indate, bid)
                todayHyUidList = pd.merge(todayHyList[todayHyList['userType'] == 1], userinfoList_pd, how='left',
                                          left_on='uid', right_on='id')  # 获取卡的等级信息根据id
                todayHyWxIdList = pd.merge(todayHyList[todayHyList['userType'] == 2], userinfoList_pd, how='left',
                                           left_on='uid', right_on='wxId')  # 获取卡的等级信息根据wxID
                todayHyUserList_pd = todayHyUidList.append(todayHyWxIdList)  # 合并二个pandas

                self.userinfo_cardLeve(todayHyUserList_pd, indate, bid)  # 卡等级用户活跃

    def get_pd_same_cloums(self, df_cloums, df2_cloums):
        '''
        返回两个list相同的部分
        :param df_cloums: 
        :param df2_cloums: 
        :return: 
        '''
        out_cloums = []
        for val in df_cloums:
            if val in df2_cloums:
                out_cloums.append(val)
        return out_cloums

    def statistical_user_age_range(self, userinfoList_pd, user_distribution, bid, indate, **kwargs):
        '''
        用户年龄区间统计
        author	: yt
        create date	: 2018-08-23
        userinfoList_pd : 需要统计的数据
        min : 年龄区间开始值
        max : 年龄区间结束值
        user_distribution : 需要把数据插入的数据表
        '''
        index = kwargs['index']
        min = kwargs['min']
        max = kwargs['max']
        type = kwargs['type']
        u_min = userinfoList_pd[userinfoList_pd['age'] >= min]
        u_max = userinfoList_pd[userinfoList_pd['age'] <= max]
        on_cloums = self.get_pd_same_cloums(u_min.columns.values.tolist(), u_max.columns.values.tolist())
        pd_on = pd.merge(u_min, u_max, on=on_cloums)

        # 判断插入日期的记录已经存在，存在isHaveSexData返回记录id,不存在返回None
        if type == 1:
            filter_map = "bid=" + str(bid) + " and typeId=2 and typeValueId=" + str(
                index) + " and userAdd=0 and userActivity=0 and indate<=" + str(indate)
            isHaveSexData = self.db_report.query(user_distribution.__table__).filter(filter_map).scalar()
            add_user_distribution = user_distribution(id=isHaveSexData, bid=bid, typeId=2, typeValueId=int(index),
                                                      userCount=int(len(pd_on)), indate=indate, intime=time.time())
        elif type == 2:
            filter_map = "bid=" + str(bid) + " and typeId=2 and typeValueId=" + str(
                index) + " and userCount=0 and userActivity=0 and indate=" + str(indate)
            isHaveSexData = self.db_report.query(user_distribution.__table__).filter(filter_map).scalar()
            add_user_distribution = user_distribution(id=isHaveSexData, bid=bid, typeId=2, typeValueId=int(index),
                                                      userAdd=int(len(pd_on)), indate=indate, intime=time.time())
        try:
            self.db_report.merge(add_user_distribution)  # 类似于add添加数据，但是主键添加存在则修改信息
            self.db_report.commit()
        except:
            self.AppLogging.warning("%d_统计年龄区间添加数据错误最开始年龄%s，结束年龄%s" % (bid, str(min), str(max)))

    def get_user_distribution(self, indate, bid, *args):
        '''
        用户分布统计（性别、年龄（未知，1
        ~18, 19
        ~21, 22
        ~25, 26
        ~30, 30
        ~35, 36
        ~40, 41
        ~50, 50 +）、卡等级）---[查询用户表]
        author	: yt
        create date	: 2018-08-17
        indate         处理数据日期
        bid            商圈id
        args           args[0]为1时统计当前日期及之前的数据，为2时只统计当前日期数据;args[1]用户pandas数据
        '''
        type = args[0]
        user_distribution = base_model_report('online_user_distribution')
        if type == 1:  # 统计当前日期及之前的数据
            inserField = 'userCount'
        elif type == 2:  # 统计当前日期数据
            inserField = 'userAdd'
        userinfoList_pd = args[1]
        # 计算性别统计--start
        pd_sex_count = userinfoList_pd.groupby(by=['sex']).size().reset_index(name='c_count')  # 去重后数据
        tmpall = []
        for index, row in pd_sex_count.iterrows():
            tmp = {}
            tmp['bid'] = int(bid)
            tmp['typeId'] = 1
            tmp['typeValueId'] = int(row['sex'])
            tmp[inserField] = int(row['c_count'])
            tmp['indate'] = indate
            tmp['intime'] = int(time.time())
            tmpall.append(tmp)
        try:
            if tmpall:
                # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
                if type == 1:
                    self.db_report.query(user_distribution).filter(user_distribution.typeId == 1,
                                                                   user_distribution.indate == indate,
                                                                   user_distribution.bid == bid).delete()
                    self.db_report.commit()
                self.db_report.execute(user_distribution.__table__.insert(), tmpall)
                self.db_report.commit()
        except:
            self.AppLogging.warning("%d_统计用户性别错误%s" % (bid, str(tmpall)))
        # 计算性别统计--end

        # 统计用户年龄区间--start
        ageRangeList = [
            {'min': 0, 'max': 0},
            {'min': 1, 'max': 18},
            {'min': 19, 'max': 21},
            {'min': 22, 'max': 25},
            {'min': 26, 'max': 30},
            {'min': 31, 'max': 35},
            {'min': 36, 'max': 40},
            {'min': 41, 'max': 50},
            {'min': 51, 'max': 150}
        ]
        for index, age in enumerate(ageRangeList):
            self.statistical_user_age_range(userinfoList_pd, user_distribution, bid, indate, index=index,
                                            min=age['min'], max=age['max'], type=type)
        # 统计用户年龄区间--end

        # 卡等级统计--start
        pd_cardLeve_count = userinfoList_pd.groupby(by=['cardLeve']).size().reset_index(name='c_count')  # 去重后数据
        cardLevetmpall = []
        for index, row in pd_cardLeve_count.iterrows():
            cardLevetmp = {}
            cardLevetmp['bid'] = int(bid)
            cardLevetmp['typeId'] = 3
            cardLevetmp['typeValueId'] = row['cardLeve']
            cardLevetmp[inserField] = int(row['c_count'])
            cardLevetmp['indate'] = indate
            cardLevetmp['intime'] = int(time.time())
            cardLevetmpall.append(cardLevetmp)

        try:
            if cardLevetmpall:
                # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
                if type == 1:
                    self.db_report.query(user_distribution).filter(user_distribution.typeId == 3,
                                                                   user_distribution.indate == indate,
                                                                   user_distribution.bid == bid).delete()
                    self.db_report.commit()
                self.db_report.execute(user_distribution.__table__.insert(), cardLevetmpall)
                self.db_report.commit()
        except:
            self.AppLogging.warning("%d_统计卡等级统计错误%s" % (bid, str(cardLevetmpall)))
        # 卡等级统计--end

        # 用户渠道来源--start
        fromArr_count = userinfoList_pd.groupby(['fromId']).count()  # 判断是否存在有数据可以分组大于零才使用
        if len(fromArr_count) > 0:
            pd_fromArr_count = userinfoList_pd.groupby(['fromId']).size().reset_index(name='c_count')  # 去重后数据
            fromArrtmpall = []
            for index, row in pd_fromArr_count.iterrows():
                fromIdtmp = {}
                fromIdtmp['bid'] = int(bid)
                fromIdtmp['typeId'] = 4
                fromIdtmp['typeValueId'] = str(row['fromId'])
                fromIdtmp[inserField] = int(row['c_count'])
                fromIdtmp['indate'] = indate
                fromIdtmp['intime'] = int(time.time())
                fromArrtmpall.append(fromIdtmp)

            try:
                if fromArrtmpall:
                    # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
                    if type == 1:
                        self.db_report.query(user_distribution).filter(user_distribution.typeId == 4,
                                                                       user_distribution.indate == indate,
                                                                       user_distribution.bid == bid).delete()
                        self.db_report.commit()
                    self.db_report.execute(user_distribution.__table__.insert(), fromArrtmpall)
                    self.db_report.commit()
            except:
                self.AppLogging.warning("%d_统计用户渠道来源错误%s" % (bid, str(fromArrtmpall)))
        # 用户渠道来源--end

        # 店铺渠道来源--start
        sidArr_count = userinfoList_pd.groupby(['sid']).count()  # 判断是否存在有数据可以分组大于零才使用
        if len(sidArr_count) > 0:
            pd_sidArr_count = userinfoList_pd[userinfoList_pd['sid'] > 0].groupby(['sid']).size().reset_index(
                name='c_count')  # 去重后数据
            sidArrtmpall = []
            for index, row in pd_sidArr_count.iterrows():
                sidTmp = {}
                sidTmp['bid'] = int(bid)
                sidTmp['typeId'] = 5
                sidTmp['typeValueId'] = str(row['sid'])
                sidTmp[inserField] = int(row['c_count'])
                sidTmp['indate'] = indate
                sidTmp['intime'] = int(time.time())
                sidArrtmpall.append(sidTmp)
            try:
                if sidArrtmpall:
                    # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
                    if type == 1:
                        self.db_report.query(user_distribution).filter(user_distribution.typeId == 5,
                                                                       user_distribution.indate == indate,
                                                                       user_distribution.bid == bid).delete()
                        self.db_report.commit()
                    self.db_report.execute(user_distribution.__table__.insert(), sidArrtmpall)
                    self.db_report.commit()
            except:
                self.AppLogging.warning("%d_商户渠道来源统计已经ok错误%s" % (bid, str(sidArrtmpall)))
        # 店铺渠道来源--end

        # 批量导入渠道来源--start
        specialCount = userinfoList_pd[userinfoList_pd['sourceFrom'] == '批量导入'][
            'sourceFrom'].count()  # 统计某个字段的某个值总量,类似于groupby
        specialArrtmpall = []
        specialTmp = {}
        specialTmp['bid'] = int(bid)
        specialTmp['typeId'] = 6
        specialTmp['typeValueId'] = "批量导入"
        specialTmp[inserField] = int(specialCount)
        specialTmp['indate'] = indate
        specialTmp['intime'] = int(time.time())
        specialArrtmpall.append(specialTmp)
        try:
            if specialArrtmpall:
                # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
                if type == 1:
                    self.db_report.query(user_distribution).filter(user_distribution.typeId == 6,
                                                                   user_distribution.indate == indate,
                                                                   user_distribution.bid == bid).delete()
                    self.db_report.commit()
                self.db_report.execute(user_distribution.__table__.insert(), specialArrtmpall)
                self.db_report.commit()
        except:
            self.AppLogging.warning("%d_商户渠道来源统计已经ok错误%s" % (bid, str(specialArrtmpall)))
        # self.write(str(specialArr_count['批量导入']))

    def integral_user_distribution(self, indate, bid):
        '''
        积分用户卡等级活跃
        author	: yt
        create date	: 2018-08-23
        indate         处理数据日期
        bid            商圈id
        '''
        user_integral_log = base_mode('online_userinfo_integral_log')
        userinfo = base_mode('online_userinfo')
        integralLog = self.db.query(user_integral_log.userId).filter(user_integral_log.bid == bid,
                                                                     user_integral_log.indate == indate).all()
        integralLog_pd = pd.DataFrame(integralLog, columns=['userId'])
        userinfoList = self.db.query(userinfo.id, userinfo.cardLeve).filter(userinfo.bid == bid).all()
        userinfoList_pd = pd.DataFrame(userinfoList, columns=['id', 'cardLeve'])
        integralList_pd = pd.merge(userinfoList_pd, integralLog_pd, how='inner', left_on='id',
                                   right_on='userId')  # 根据用户合并取其交集

        user_distribution = base_model_report('online_user_distribution')

        # 积分用户卡等级统计--start
        pd_cardLeve_count = integralList_pd.groupby(['cardLeve']).size().reset_index(name="c_count")  # 去重后数据
        cardLevetmpall = []

        for index, row in pd_cardLeve_count.iterrows():
            cardLevetmp = {}
            cardLevetmp['bid'] = int(bid)
            cardLevetmp['typeId'] = 7
            cardLevetmp['typeValueId'] = row['cardLeve']
            cardLevetmp['userCount'] = int(row['c_count'])
            cardLevetmp['indate'] = indate
            cardLevetmp['intime'] = int(time.time())
            cardLevetmpall.append(cardLevetmp)

        try:
            if cardLevetmpall:
                # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
                self.db_report.query(user_distribution).filter(user_distribution.typeId == 7,
                                                               user_distribution.indate == indate,
                                                               user_distribution.bid == bid).delete()
                self.db_report.commit()
                self.db_report.execute(user_distribution.__table__.insert(), cardLevetmpall)
                self.db_report.commit()
        except:
            self.AppLogging.warning("%d_积分用户统计卡等级统计错误%s" % (bid, str(cardLevetmpall)))
        # 积分用户卡等级统计--end

    def integralAddUse_SumCount(self, indate, bid, userinfoList_pd):
        '''
        计算积分-积分增加额度和笔数-积分使用额度和笔数
        author	: yt
        create date	: 2018-08-24
        indate         处理数据日期
        bid            商圈id
        '''
        user_integral_log = base_mode('online_userinfo_integral_log')
        allIntegral = userinfoList_pd['integral'].sum()  # 总剩余积分
        # 读取积分记列表
        integralList = self.db.query(user_integral_log.userId, user_integral_log.money, user_integral_log.integral,
                                     user_integral_log.changeType).filter(user_integral_log.indate == indate,
                                                                          user_integral_log.bid == bid).all()
        integralList_pd = pd.DataFrame(integralList, columns=['userId', 'money', 'integral', 'changeType'])

        pd_integral_add_sum = integralList_pd[integralList_pd['changeType'] == 1].sum()  # 积分增加计算
        pd_integral_use_sum = integralList_pd[integralList_pd['changeType'] == 2].sum()  # 积分减少计算

        addUserCount = integralList_pd[integralList_pd['changeType'] == 1].groupby(['userId']).size().count()  # 积分增加用户数
        useUserCount = integralList_pd[integralList_pd['changeType'] == 2].groupby(['userId']).size().count()  # 积分减少用户数
        moneyCount = integralList_pd[integralList_pd['money'] > 0]['money'].count()  # 消费笔数
        moneyUserCount = integralList_pd[integralList_pd['money'] > 0].groupby(['userId']).size().count()  # 消费人数
        activeCount = integralList_pd['userId'].count()  # 积分活跃人次
        activeUserCount = integralList_pd.groupby(['userId']).size().count()  # 积分活跃人数

        tmp = {}
        tmp['bid'] = int(bid)
        tmp['allIntegral'] = int(allIntegral)  # 总剩余积分
        tmp['moneySum'] = int(pd_integral_use_sum['money'])  # 消费总金额
        tmp['moneyCount'] = int(moneyCount)  # 消费笔数
        tmp['moneyUserCount'] = int(moneyUserCount)  # 消费人数
        tmp['addSum'] = int(pd_integral_add_sum['integral'])  # 增加积分总数
        tmp['addCount'] = int(pd_integral_add_sum['changeType'])  # 增加笔数
        tmp['addUserCount'] = int(addUserCount)  # 增加人数
        tmp['useSum'] = int(pd_integral_use_sum['integral'])  # 使用积分总数
        tmp['useCount'] = int(pd_integral_use_sum['changeType'] / 2)  # 使用笔数

        tmp['useUserCount'] = int(useUserCount)  # 使用人数
        tmp['activeCount'] = int(activeCount)  # 积分活跃人次
        tmp['activeUserCount'] = int(activeUserCount)  # 积分活跃人数
        tmp['indate'] = indate
        tmp['intime'] = int(time.time())
        user_integral = base_model_report('online_user_integral')
        try:
            # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
            self.db_report.query(user_integral).filter(user_integral.indate == indate,
                                                       user_integral.bid == bid).delete()
            self.db_report.commit()
            self.db_report.execute(user_integral.__table__.insert(), tmp)
            self.db_report.commit()
        except:
            self.AppLogging.warning("%d_积分增加额度和笔数统计错误%s" % (bid, str(tmp)))

    def integral_cardLeve(self, indate, bid, userinfoList_pd):
        '''
        :卡等级用户积分占比趋势（柱状占比）
        :author  : yt
	    :create date : 2018-08-24
        :param indate: 统计数据日期
        :param bid: 商圈id
        :return:
        '''
        userinfo = base_mode('online_userinfo')
        userinfo_member_card = base_mode('online_userinfo_member_card')
        integral_card_level = base_model_report('online_integral_card_level')
        card_cate_list = self.db.query(userinfo_member_card.cardLeve).filter(userinfo_member_card.bid == bid,
                                                                             userinfo_member_card.status == 1).all()
        card_cate_list_pd = pd.DataFrame(card_cate_list, columns=['cardLeve'])
        merge_pd = pd.merge(card_cate_list_pd, userinfoList_pd, on="cardLeve")  # 合并取值
        tmpall = []
        for index, row in card_cate_list_pd.iterrows():
            sumStr = merge_pd[merge_pd['cardLeve'] == row['cardLeve']]['integral'].sum()  # 取各个卡等级的积分总值
            tmp = {}
            tmp['bid'] = bid
            tmp['cardLeve'] = row['cardLeve']
            tmp['sumIntegral'] = int(sumStr)
            tmp['intime'] = int(time.time())
            tmp['indate'] = indate
            tmpall.append(tmp)
        try:
            if tmpall:
                # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
                self.db_report.query(integral_card_level).filter(integral_card_level.indate == indate,
                                                                 integral_card_level.bid == bid).delete()
                self.db_report.commit()
                self.db_report.execute(integral_card_level.__table__.insert(), tmpall)
                self.db_report.commit()
        except:
            self.AppLogging.warning("%d_卡等级用户积分占比趋势统计错误%s" % (bid, str(tmpall)))

    def integralShop(self, indate, bid):
        '''
        积分商城
        商品预定数---[查询商品预定log]兑换没有核销
        商品核销数---[查询核销log表]
        商品预定排行---[查询商品预定log]（前20个商品）时间区间排行
        商品核销排行---[查询核销log表]（前20个商品）时间区间排行
        :author : yt
        :create date    : 2018-08-24
        :param indate:  数据日期
        :param bid: 商圈id
        :return:
        '''
        integral_order_log = base_mode("online_integral_order_log")
        integral_goods = base_mode("online_integral_goods")
        integral_shop = base_model_report("online_integral_shop")
        order_log_list = self.db.query(integral_order_log.userId, integral_order_log.goodId,
                                       integral_order_log.buyCount, integral_order_log.integralOrderStatus).filter(
            integral_order_log.indate == indate, integral_order_log.bid == bid).all()

        order_log_list_pd = pd.DataFrame(order_log_list,
                                         columns=['userId', 'goodId', 'buyCount', 'integralOrderStatus'])
        orderCount = order_log_list_pd[order_log_list_pd['integralOrderStatus'] == 1]['buyCount'].sum()  # 下单总量
        checkCount = order_log_list_pd[order_log_list_pd['integralOrderStatus'] == 3]['buyCount'].sum()  # 已核销总量

        orderUserCount = order_log_list_pd[order_log_list_pd['integralOrderStatus'] == 1].groupby(
            ['userId']).size().count()
        cancelUserCount = order_log_list_pd[order_log_list_pd['integralOrderStatus'] == 3].groupby(
            ['userId']).size().count()

        integral_goods_List = self.db.query(integral_goods.stock).filter(integral_goods.bid == bid,
                                                                         integral_goods.isDel == 0).all()
        allStork = pd.DataFrame(integral_goods_List, columns=['stock'])['stock'].sum()  # 积分商城商品库存
        shoptmpAll = []
        shoptmp = {}
        shoptmp['bid'] = bid
        shoptmp['allStork'] = int(allStork)  # 总库存
        shoptmp['orderUserCount'] = int(orderUserCount)
        shoptmp['orderCount'] = int(orderCount)
        shoptmp['cancelUserCount'] = int(cancelUserCount)
        shoptmp['cancelCount'] = int(checkCount)
        shoptmp['indate'] = indate
        shoptmp['intime'] = int(time.time())
        shoptmpAll.append(shoptmp)

        try:
            if shoptmpAll:
                # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
                self.db_report.query(integral_shop).filter(integral_shop.indate == indate,
                                                           integral_shop.bid == bid).delete()
                self.db_report.commit()
                self.db_report.execute(integral_shop.__table__.insert(), shoptmpAll)
                self.db_report.commit()
        except:
            self.AppLogging.warning("%d_积分商城统计错误%s" % (bid, str(shoptmpAll)))

        # 订购排行，降序排行
        integral_shop_order_ranking = base_model_report('online_integral_shop_order_ranking')
        buyRank = order_log_list_pd[order_log_list_pd['integralOrderStatus'] == 1].groupby(
            ['goodId']).size().reset_index(name='c_count').sort_values(by='c_count', ascending=False)
        buyRankTop20 = buyRank[0:20]  # 获取排行前20
        ranktmpArr = []
        for index, row in buyRankTop20.iterrows():
            ranktmp = {}
            ranktmp['bid'] = bid
            ranktmp['goodId'] = int(row['goodId'])
            ranktmp['buyCount'] = int(row['c_count'])
            ranktmp['indate'] = indate
            ranktmp['intime'] = time.time()
            ranktmpArr.append(ranktmp)

        try:
            if ranktmpArr:
                # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
                self.db_report.query(integral_shop_order_ranking).filter(integral_shop_order_ranking.indate == indate,
                                                                         integral_shop_order_ranking.bid == bid).delete()
                self.db_report.commit()
                self.db_report.execute(integral_shop_order_ranking.__table__.insert(), ranktmpArr)
                self.db_report.commit()

        except:
            self.AppLogging.warning("%d_积分商城订购排行统计错误%s" % (bid, str(ranktmpArr)))

        # 核销排行，降序排行
        integral_shop_cancel_ranking = base_model_report('online_integral_shop_cancel_ranking')
        checkRank = order_log_list_pd[order_log_list_pd['integralOrderStatus'] == 3].groupby(
            ['goodId']).size().reset_index(name='c_count').sort_values(by='c_count', ascending=False)
        checkRankTop20 = checkRank[0:20]  # 获取排行前20
        checkRanktmpArr = []
        for index, row in checkRankTop20.iterrows():
            checkRanktmp = {}
            checkRanktmp['bid'] = bid
            checkRanktmp['goodId'] = int(row['goodId'])
            checkRanktmp['buyCount'] = int(row['c_count'])
            checkRanktmp['indate'] = indate
            checkRanktmp['intime'] = time.time()
            checkRanktmpArr.append(checkRanktmp)

        try:
            if ranktmpArr:
                # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
                self.db_report.query(integral_shop_cancel_ranking).filter(
                    integral_shop_cancel_ranking.indate == indate,
                    integral_shop_cancel_ranking.bid == bid).delete()
                self.db_report.commit()
                self.db_report.execute(integral_shop_cancel_ranking.__table__.insert(), checkRanktmpArr)
                self.db_report.commit()

        except:
            self.AppLogging.warning("%d_积分商城核销排行统计错误%s" % (bid, str(checkRanktmpArr)))

    def messageShow(self, indate, bid):
        '''
        :desc: 计算消息--消息发送次数
        :author: yt
        :create date: 2018-08-27
        :param indate: 数据日期
        :param bid: 商圈id
        :return:
        '''
        message_log = base_mode("online_message_list")
        message = base_model_report("online_message")
        messageList = self.db.query(message_log.errorCode, message_log.sendType, message_log.fromType).filter(
            message_log.bid == bid, message_log.indate == indate).all()
        messageList_pd = pd.DataFrame(messageList, columns=['errorCode', 'sendType', 'fromType'])

        sendCount = messageList_pd['sendType'].count()  # 发送次数
        sendSuccessCount = messageList_pd[messageList_pd['errorCode'] == 0]['errorCode'].count()  # 发送成功
        smsCount = messageList_pd[messageList_pd['sendType'] == 1]['sendType'].count()  # 手机短信
        wxtemplateCount = messageList_pd[messageList_pd['sendType'] == 2]['sendType'].count()  # 微信模板消息
        integralCount = messageList_pd[messageList_pd['fromType'] == 1]['fromType'].count()  # 积分
        counponsCount = messageList_pd[messageList_pd['fromType'] == 4]['fromType'].count()  # 卡券
        gameCount = messageList_pd[messageList_pd['fromType'] == 5]['fromType'].count()  # 组件
        marketCount = messageList_pd[messageList_pd['fromType'] == 6]['fromType'].count()  # 营销
        isHaveData = self.db_report.query(message.__table__).filter(message.bid == bid, message.indate).scalar()
        add_message = message(id=isHaveData, bid=bid, sendCount=int(sendCount), smsCount=int(smsCount),
                              sendSuccessCount=int(sendSuccessCount), wxtemplateCount=int(wxtemplateCount),
                              counponsCount=int(counponsCount), integralCount=int(integralCount),
                              gameCount=int(gameCount), marketCount=int(marketCount), indate=indate,
                              intime=time.time())
        try:
            self.db_report.merge(add_message)
            self.db_report.commit()
        except:
            self.AppLogging.warning("%d_计算消息统计错误%s" % (bid, str(messageList_pd)))

    def autoIntegral(self, indate, bid):
        '''
        :desc: 自助积分统计：数据统计包含：拍照积分、扫码积分、已审核，未审核
        :author: yt
        :create date: 2018-08-28
        :param indate: 数据日期
        :param bid: 商圈id
        :return:
        '''
        indate = 1498752000
        auto_integral = base_mode("online_auto_integral")
        business_shops = base_mode("online_business_shops")
        auto_integral_report = base_model_report("online_auto_integral")

        # 获取当前商圈可用状态的店铺
        shop_list = self.db.query(business_shops.id, business_shops.shopName).filter(business_shops.bid == bid,
                                                                                     business_shops.status == 1,
                                                                                     business_shops.isDel == 0).all()
        shop_list_pd = pd.DataFrame(shop_list, columns=['id', 'shopName'])

        # 获取当前日期商圈的自助积分列表
        auto_integral_list = self.db.query(auto_integral.id, auto_integral.sid, auto_integral.type,
                                           auto_integral.status).filter(auto_integral.bid == bid,
                                                                        auto_integral.indate == indate).all()
        auto_integral_list_pd = pd.DataFrame(auto_integral_list, columns=['id', 'sid', 'type', 'status'])

        tmpArr = []
        for index, row in shop_list_pd.iterrows():
            scavenging_integral = auto_integral_list_pd[(auto_integral_list_pd['sid'] == row['id']) & (
                    auto_integral_list_pd['type'] == 1)]['id'].count()  # 获取扫码当天扫码积分总数目
            photo_integral = auto_integral_list_pd[(auto_integral_list_pd['sid'] == row['id']) & (
                    auto_integral_list_pd['type'] == 2)]['id'].count()  # 获取当天上传积分总数目

            checkCount = auto_integral_list_pd[
                (auto_integral_list_pd['sid'] == row['id']) & (auto_integral_list_pd['type'] == 2) & (
                        auto_integral_list_pd['status'] == 2)]['id'].count()  # 获取已审核总数量

            noCheckCount = auto_integral_list_pd[
                (auto_integral_list_pd['sid'] == row['id']) & (auto_integral_list_pd['type'] == 2) & (
                        auto_integral_list_pd['status'] == 1)]['id'].count()  # 获取未审核总数量
            tmp = {}
            tmp['bid'] = bid
            tmp['shop_id'] = int(row['id'])
            tmp['photo_integral'] = int(photo_integral)
            tmp['scavenging_integral'] = int(scavenging_integral)
            tmp['cancelCount'] = int(checkCount)
            tmp['noCancelCount'] = int(noCheckCount)
            tmp['intime'] = time.time()
            tmp['indate'] = indate
            tmpArr.append(tmp)
        try:
            # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
            self.db_report.query(auto_integral_report).filter(auto_integral_report.indate == indate,
                                                              auto_integral_report.bid == bid).delete()
            self.db_report.commit()
            self.db_report.execute(auto_integral_report.__table__.insert(), tmpArr)
            self.db_report.commit()
        except:
            self.AppLogging.warning("%d_前日期商圈的自助积分统计错误%s" % (bid, str(tmpArr)))

        # 所有扫码积分数，这里不等于商铺Id的总和，因为数据可能不存在商铺id
        all_scavenging_integral = auto_integral_list_pd[(auto_integral_list_pd['type'] == 1)][
            'id'].count()  # 获取扫码当天扫码积分总数目
        all_photo_integral = auto_integral_list_pd[(auto_integral_list_pd['type'] == 2)]['id'].count()  # 获取当天上传积分总数目
        all_checkCount = \
            auto_integral_list_pd[(auto_integral_list_pd['type'] == 2) & (auto_integral_list_pd['status'] == 2)][
                'id'].count()  # 获取已审核总数量
        all_noCheckCount = \
            auto_integral_list_pd[(auto_integral_list_pd['type'] == 2) & (auto_integral_list_pd['status'] == 1)][
                'id'].count()  # 获取未审核总数量

        all_add_integral = auto_integral_report(bid=bid, shop_id=int(-1), photo_integral=int(all_photo_integral),
                                                scavenging_integral=int(all_scavenging_integral),
                                                cancelCount=int(all_checkCount),
                                                noCancelCount=int(all_noCheckCount), intime=time.time(),
                                                indate=indate)
        try:
            self.db_report.add(all_add_integral)
            self.db_report.commit()
        except:
            arr = [str(all_scavenging_integral), str(all_photo_integral), str(all_checkCount), str(all_noCheckCount)]
            jsonObj = json.dumps(arr)
            self.AppLogging.warning("%d_前日期商圈的自助积分统计错误%s :" % (bid, str(jsonObj)))

    def electBindingUser(self, indate, bid, userinfoList_pd, userinfoDayList_pd):
        '''
        :Desc:会员数:包括电子会员和绑定的卡会员---[查询用户表]isRealCard=1实体卡 电子会员增长、绑卡会员的增长、绑卡会员的解绑
        :author: yt
	    :create date: 2018-08-29
        :param indate: 数据日期
        :param bid: 商圈id
        :param userinfoDayList_pd: 当前日期用户数据
        :return:
        '''
        # 根据各商圈卡等级查电子会员和绑卡会员
        aBusinessUser = base_mode('online_a_business_user')
        userinfo_member_card = base_mode('online_userinfo_member_card')
        user_active = base_model_report('online_user_active')
        card_cate_list = self.db.query(userinfo_member_card.cardLeve, userinfo_member_card.is_default).filter(
            userinfo_member_card.bid == bid,
            userinfo_member_card.status == 1).all()
        card_cate_list_pd = pd.DataFrame(card_cate_list, columns=['cardLeve', 'is_default'])

        isRealCard = self.db.query(aBusinessUser).filter(aBusinessUser.id == bid).first().isRealCard
        if isRealCard == 0:  # 存在实体卡
            electUserCount = userinfoList_pd['id'].count()
            electUserAdd = userinfoDayList_pd['id'].count()
            bindingUserCount = 0
            bindingUserAdd = 0
        else:  # 不存在实体卡
            electUserAdd = 0
            bindingUserAdd = 0
            try:
                isDefaultCardLeve = card_cate_list_pd[card_cate_list_pd['is_default'] == 1]['cardLeve'][0]
                electUserCount = userinfoList_pd[userinfoList_pd['cardLeve'] == isDefaultCardLeve]['id'].count()
                bindingUserCount = userinfoList_pd[userinfoList_pd['cardLeve'] != isDefaultCardLeve]['id'].count()

                # 当前日期增长会员
                electUserAdd = userinfoDayList_pd[userinfoDayList_pd['cardLeve'] == isDefaultCardLeve]['id'].count()
                bindingUserAdd = userinfoDayList_pd[userinfoDayList_pd['cardLeve'] != isDefaultCardLeve]['id'].count()
            except:
                electUserCount = 0
                bindingUserCount = 0

        userinfoDb = base_mode('online_userinfo')
        cancelBindingUserList = self.db.query(userinfoDb.cancelRealCardDate).filter(userinfoDb.bid == bid,
                                                                                    userinfoDb.cancelRealCardDate == indate).all()
        cancelBindingUserList_pd = pd.DataFrame(cancelBindingUserList, columns=['cancelRealCardDate'])
        cancelBindingUserAdd = cancelBindingUserList_pd['cancelRealCardDate'].count()

        isHave = self.db_report.query(user_active.__table__).filter(user_active.bid == bid,
                                                                    user_active.indate == indate).scalar()
        add_user_active = user_active(id=isHave, bid=bid, electUserCount=int(electUserCount),
                                      bindingUserCount=int(bindingUserCount), electUserAdd=int(electUserAdd),
                                      bindingUserAdd=int(bindingUserAdd),
                                      cancelBindingUserAdd=int(cancelBindingUserAdd), intime=time.time(), indate=indate)
        try:
            self.db_report.merge(add_user_active)
            self.db_report.commit()
        except:
            self.AppLogging.warning("%d_商圈会员数统计错误:" % bid)

    def outDayHuoyue(self, todayHyList_pd, user_active, indate, bid):
        '''
        :desc: 计算商圈天活跃
        :author: yt
        :create date: 2018-08-29
        :param todayHyList: 商圈一天的所有访问记录
        :param user_active: 数据库对象
        :param indate:  当前日期
        :param bid: 商圈id
        :return: []
        '''

        allcount = todayHyList_pd['id'].count()
        ccount = todayHyList_pd.groupby(['uid']).size().count()

        isHave = self.db_report.query(user_active.__table__).filter(user_active.bid == bid,
                                                                    user_active.indate == indate).scalar()
        add_user_active = user_active(id=isHave, bid=bid, allcount=int(allcount), ccount=int(ccount),
                                      intime=time.time(), indate=indate)
        try:
            self.db_report.merge(add_user_active)
            self.db_report.commit()


        except:
            self.AppLogging.warning("%d_商圈天活跃统计错误:" % bid)

        ret = todayHyList_pd.groupby(['uid']).tail(1)  # 获取分组之后每组的最后一行记录信息
        return ret

    def userinfo_cardLeve(self, todayHyUserList_pd, indate, bid):
        '''
        :desc:卡等级用户活跃：以”卡等级“为维度统计会员活跃---[查询活跃表]取出商圈的卡等级后过滤活跃用户，卡等级个数的连续柱状图 online_userinfo_member_card cardLeve
        :author: yt
	    :create date: 2018-08-29
        :param todayHyUserList: 过滤后的用户活跃数据
        :param indate: 数据日期
        :param bid: 商圈id
        :return:
        '''
        userinfo_member_card = base_mode("online_userinfo_member_card")
        card_cate_list = self.db.query(userinfo_member_card.cardLeve).filter(userinfo_member_card.bid == bid,
                                                                             userinfo_member_card.status == 1).all()
        card_cate_list_pd = pd.DataFrame(card_cate_list, columns=['cardLeve'])

        out_card_cater = todayHyUserList_pd.groupby(['cardLeve']).size().reset_index(name='c_count')

        # print(out_card_cater)
        # print(out_card_cater[out_card_cater['cardLeve'].isin(['黑金卡'])])
        # return
        activity_card_level = base_model_report("online_activity_card_level")

        tmpArr = []
        for index, row in card_cate_list_pd.iterrows():
            tmp = {}
            tmp['bid'] = bid
            tmp['cardLeve'] = row['cardLeve']
            tmp['indate'] = indate
            tmp['intime'] = time.time()
            isHave = out_card_cater[out_card_cater['cardLeve'].isin([row['cardLeve']])]
            if isHave.empty:  # 不存在
                tmp['userCount'] = 0
            else:  # 存在取出对应卡等级活跃用户数量
                tmp['userCount'] = int(out_card_cater[out_card_cater['cardLeve'] == row['cardLeve']]['c_count'])

            tmpArr.append(tmp)


        try:
            # 判断当前日期是否已存在,数据已存在执行删除后在执行添加
            self.db_report.query(activity_card_level).filter(activity_card_level.indate == indate,
                                                             activity_card_level.bid == bid).delete()
            self.db_report.commit()
            self.db_report.execute(activity_card_level.__table__.insert(), tmpArr)
            self.db_report.commit()
        except:
            self.AppLogging.warning("%d_前日期商圈的卡等级用户活跃统计错误%s" % (bid, str(tmpArr)))
