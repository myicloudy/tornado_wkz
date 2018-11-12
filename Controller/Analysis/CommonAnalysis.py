from Controller.Control import ControlHandler
from Model.shopmall_main_db.base_model import base_mode
import pandas as pd
import gc
import datetime
import time
import string
import calendar
pd.set_option('display.max_columns', None)  # 显示完整列
# pd.set_option('display.max_rows', None)  # 设置显示完整行数
# @Explain : Home文件公共方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg


'''
:param returnArr: 上一次返回的数据
:param bid: 商圈id
:param indate: 当前日期
:param params: 节点参数
:param otherParams: 配置节点数据库查出集合
:param nodeParams: 主程序传过来数据
:return: returnArr 数据集合
'''


class CommonAnalysis(ControlHandler):
    def empty_pd(self):
        '''空pandas对象'''
        return pd.DataFrame()

    def get_userinfo_bid(self, bid):
        '''
        获取bid的用户数据
        :param bid: 
        :return: 
        '''

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

    def time_prev_mouth(self, indate, mouths=1):
        '''
        传入时间戳返回上个月的时间戳
        :param indate: 
        :return: 
        '''

        indate_timestamp = datetime.datetime.fromtimestamp(indate)
        last_month = indate_timestamp.month - mouths
        last_year = indate_timestamp.year
        if last_month == 0:
            last_month = 12
            last_year -= 1
        if last_month > 12:
            last_year += 1
            last_month -= 12
        elif last_month < 1:
            last_year -= 1
            last_month += 12

        day = min(calendar.monthrange(last_year, last_month)[1],indate_timestamp.day)
        month_time = datetime.datetime(month=last_month, year=last_year, day=day)
        return int(time.mktime(month_time.timetuple()))

    def get_str_to_stime(self, indate, evalArr):
        '''
        时间字符串转换成时间
        :param indate: 
        :return: 
        '''
        outtime = indate  # 出错默认返回数据

        try:
            if 'dayStr' not in evalArr:
                self.AppLogging.error('1.计算时间时不含有dayStr字符串%s' % evalArr)
                return outtime
        except:
            self.AppLogging.error('2.计算时间时不含有dayStr字符串%s' % evalArr)
            return outtime
        dayStr = evalArr['dayStr']
        if dayStr == '-1 week':
            return indate - 86400 * (7 + 1)
        elif dayStr == '-2 week':
            return indate - 86400 * (14 + 1)
        elif dayStr == '-3 week':
            return indate - 86400 * (21 + 1)
        elif dayStr == '-1 month':
            return self.time_prev_mouth(indate, 1)
        elif dayStr == '-1 mouth':
            return self.time_prev_mouth(indate, 1)
        elif dayStr == '-2 month':
            return self.time_prev_mouth(indate, 2)
        elif dayStr == '-3 month':
            return self.time_prev_mouth(indate, 3)
        elif dayStr == '-1 day':
            return indate - 86400 * (1 + 1)
        elif dayStr == '-2 day':
            return indate - 86400 * (2 + 1)
        elif dayStr == '-3 day':
            return indate - 86400 * (3 + 1)
        else:
            self.AppLogging.error('dayStr字符串不在判断范围内%s' % evalArr)
            return outtime

    def evalOtherParams(self, str):
        '''
        参数转换变量
        * author		: gg
        * creat date	: 2018/7/20
        '''
        if str:
            str = str.replace('array(', '{').replace(')', '}').replace('=>', ':')
            try:
                outStr = eval(str)
                return outStr
            except:
                return {}

    def sql_userinfo_pd(self, sql):
        '''
        指定sql查询用户信息返回pd对象
        :param sql: 
        :return: 
        '''
        sql = 'select id,userPhone from online_userinfo where ' + sql
        ulist = self.db.execute(sql)
        count = ulist.rowcount
        if count == 0:
            return self.empty_pd()
        # 查询数据后转换成list默认是一个迭代器cardLeve
        tmp = [[]] * count
        i = 0
        if ulist:
            for val in ulist:
                tmp2 = []
                tmp2.append(val.id)
                tmp2.append(val.userPhone)
                tmp[i] = tmp2.copy()
                i = i + 1
        else:
            return self.empty_pd()
        userinfo_pd = pd.DataFrame(tmp, columns=['uid', 'userPhone'])
        userinfo_pd['userType'] = 1
        userinfo_pd['userId'] = userinfo_pd['uid']
        userinfo_pd['infoData'] = 1
        print('userinfo_pd==', len(userinfo_pd))
        # 销毁变量 预防内存过高
        del ulist, tmp, tmp2
        gc.collect()
        return userinfo_pd

    def sql_userinfo_weixin_pd(self, sql):
        '''
        指定sql查询用户微信信息返回pd对象
        :param sql: 
        :return: 
        '''
        sql = 'select id,userId from online_userinfo_weixin where ' + sql
        ulist = self.db.execute(sql)
        count = ulist.rowcount
        if count == 0:
            return self.empty_pd()
        # 查询数据后转换成list默认是一个迭代器cardLeve
        tmp = [[]] * count
        i = 0
        if ulist:
            for val in ulist:
                tmp2 = []
                tmp2.append(val.id)
                tmp2.append(val.userId)
                tmp[i] = tmp2.copy()
                i = i + 1
        else:
            return self.empty_pd()
        userinfo_pd = pd.DataFrame(tmp, columns=['uid', 'userId'])
        print('userinfo_weixin_pd==', len(userinfo_pd))
        # 销毁变量 预防内存过高
        del ulist, tmp, tmp2
        gc.collect()
        return userinfo_pd

    def analysis_userSex(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        用户性别
        * author		: gg
        * creat date	: 2018/7/20
        '''
        # self.write(otherParams.otherParams)
        logStr = '当前计算是：用户性别<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        sql_where = 'sex=' + str(evalArr['sexType']) + ' and bid=' + str(bid) + ' and userPhone<>0 and indate<=' + str(
            indate)
        _list_all = self.sql_userinfo_pd(sql_where)  # 通过sql返回通用数据
        logStr = logStr + '从数据库中取出数据:' + str(len(_list_all)) + '<br>'
        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_selectCardLevel(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡等级
        * author		: gg
        * creat date	: 2018/7/17 
        '''
        logStr = '当前计算是：卡等级<br>'

        if not params:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        sql = ''  # 拼接卡等级sql
        for params_value in params:
            a = 1
            tmp = "cardLeve='" + str(params_value) + "'" if not sql else " or cardLeve='" + str(params_value) + "'"
            sql = sql + tmp
        if not sql:
            logStr = logStr + 'err--拼装sql出错<br>'
            return self.empty_pd(), logStr
        else:
            sql_where = '(' + sql + ') and bid=' + str(bid) + ' and userPhone<>0'
        _list_all = self.sql_userinfo_pd(sql_where)  # 通过sql返回通用数据
        logStr = logStr + '从数据库中取出数据:' + str(len(_list_all)) + '<br>'
        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # for params_value in params:
                #     _list.append(ulist_pd[ulist_pd['cardLeve'] == str(card_cateArr[int(params_value)])])
                # _list_all = pd.concat(_list, ignore_index=True)  # 两个pandas数据拼接
                # print(len(cardLeve_list_all))

        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_user_age(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        用户年龄段
        * author		: gg
        * creat date	: 2018/7/23
        '''
        # self.write(otherParams.otherParams)
        logStr = '当前计算是：用户年龄段<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        sql_where = ''
        if evalArr['agemax'] == -1:
            sql_where = 'age>=' + str(evalArr['agemin'])
        else:
            sql_where = 'age>=' + str(evalArr['agemin']) + ' and age<=' + str(evalArr['agemax'])
        sql_where = sql_where + ' and bid=' + str(
            bid) + ' and userPhone<>0 and indate<=' + str(
            indate)
        _list_all = self.sql_userinfo_pd(sql_where)  # 通过sql返回通用数据
        logStr = logStr + '从数据库中取出数据:' + str(len(_list_all)) + '<br>'
        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_integral_active(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        积分活跃
        * author		: gg
        * creat date	: 2018/7/23
        '''
        # self.write(otherParams.otherParams)
        logStr = '当前计算是：积分活跃<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr

        # 时间字符串转换成时间
        stime = self.get_str_to_stime(indate, evalArr)
        sql_where = 'bid=' + str(bid) + ' and indate>=' + str(stime)
        sql = 'select userId from online_userinfo_integral_log where ' + sql_where
        logStr = logStr + '取数据时间：' + time.strftime("%Y-%m-%d", time.localtime(stime)) + '<br>'
        # sql查询出数据放到二维数组并且转换为pd对象
        ulist = self.db.execute(sql)
        count = ulist.rowcount
        logStr = logStr + '从数据库中取出数据:' + str(count) + '<br>'
        # 查询数据后转换成list默认是一个迭代器cardLeve
        tmp = [[]] * count
        i = 0
        if ulist:
            for val in ulist:
                tmp2 = []
                tmp2.append(val.userId)
                tmp[i] = tmp2.copy()
                i = i + 1

        userinfo_pd = pd.DataFrame(tmp, columns=['userId'])
        logStr = logStr + '实例化到Pandas中数据有:' + str(len(userinfo_pd)) + '<br>'
        gp = userinfo_pd.groupby(by=['userId'])
        newdf = gp.size()  # 去重后数据
        pd_count = newdf.reset_index(name='count1')
        logStr = logStr + 'userId去重后活跃数据有:' + str(len(pd_count)) + '<br>'
        # print(pd_count)
        # return
        pd_count['userType'] = 1
        pd_count['uid'] = pd_count['userId']
        pd_count['infoData'] = 1
        # print(len(pd_count))
        # 销毁变量 预防内存过高
        del ulist, tmp, tmp2, userinfo_pd, gp
        gc.collect()
        min = evalArr['min']
        max = evalArr['max']
        # 准备好所有用户数据
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'
        if min == -1 and max == -1:  # 沉默：最近无任何积分变动
            # 无活跃，把所有数据拿出来不在内的数据

            # print('userinfo_all_pd=', len(userinfo_all_pd))
            # 取两个pd的差集，左边是全数组，append是最好是一个列的数据，不然多出来的咧会有NaN，不好处理了
            userId_pd = pd.DataFrame(pd_count['userId'], columns=['userId'])
            userinfo_all_pd = userinfo_all_pd.append(userId_pd)
            _list_all = userinfo_all_pd.drop_duplicates(subset=['userId'], keep=False)
            logStr = logStr + '活跃数据和所有用户数据得出差集就是沉默用户数:' + str(len(_list_all)) + '<br>'
        elif max == -1:  # 高：最近有5日+产生积分变动
            pd_max = pd_count[pd_count['count1'] >= max]
            on_cloums = self.get_pd_same_cloums(pd_max.columns.values.tolist(), userinfo_all_pd.columns.values.tolist())
            _list_all = pd.merge(userinfo_all_pd, pd_max, on=on_cloums)
            logStr = logStr + '活跃数据大于' + str(max) + '的数据有:' + str(len(_list_all)) + '<br>'
        else:  # 区间活跃
            u_min = pd_count[pd_count['count1'] >= min]
            u_max = pd_count[pd_count['count1'] <= max]
            on_cloums = self.get_pd_same_cloums(u_min.columns.values.tolist(), u_max.columns.values.tolist())
            pd_on = pd.merge(u_min, u_max, on=on_cloums)
            on_cloums = self.get_pd_same_cloums(pd_on.columns.values.tolist(), userinfo_all_pd.columns.values.tolist())
            _list_all = pd.merge(userinfo_all_pd, pd_on, on=on_cloums)
            logStr = logStr + '活跃数据大于' + str(min) + '并且小于' + str(max) + '的数据有:' + str(len(_list_all)) + '<br>'
        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_integral_between(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        积分区间
        * author		: gg
        * creat date	: 2018/7/24
        '''
        # self.write(otherParams.otherParams)
        logStr = '当前计算是：积分区间<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        # 取出积分换算单位
        business_integral_ratio = base_mode('online_business_integral_ratio')
        ratioFind = self.db.query(business_integral_ratio).filter(business_integral_ratio.bid == bid).first()
        if not hasattr(ratioFind, 'id'):
            coefficient = 1  # 系数默认1

        coefficient = round(ratioFind.rmb / ratioFind.integral, 2)
        if coefficient <= 0:
            coefficient = 1
        logStr = logStr + '当前积分系数是' + str(coefficient) + '<br>'

        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'

        sql = 'select id,userPhone,integral from online_userinfo where ' + sql_where
        ulist = self.db.execute(sql)
        count = ulist.rowcount
        # 查询数据后转换成list默认是一个迭代器cardLeve
        tmp = [[]] * count
        i = 0
        if ulist:
            for val in ulist:
                tmp2 = []
                tmp2.append(val.id)
                tmp2.append(val.userPhone)
                tmp2.append(val.integral)
                tmp[i] = tmp2.copy()
                i = i + 1
        userinfo_pd = pd.DataFrame(tmp, columns=['uid', 'userPhone', 'integral'])
        userinfo_pd['userType'] = 1
        userinfo_pd['userId'] = userinfo_pd['uid']
        userinfo_pd['infoData'] = 1
        userinfo_pd['integral'] = userinfo_pd['integral'] * coefficient  # 积分乘以系数
        # 销毁变量 预防内存过高
        del ulist, tmp, tmp2
        gc.collect()

        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_pd)) + '<br>'
        min = evalArr['min']
        max = evalArr['max']
        if max == -1:  # 10001+
            pd_max = userinfo_pd[userinfo_pd['integral'] >= min]
            on_cloums = self.get_pd_same_cloums(pd_max.columns.values.tolist(), userinfo_pd.columns.values.tolist())
            _list_all = pd.merge(userinfo_pd, pd_max, on=on_cloums)
            logStr = logStr + '积分大于' + str(min) + '的数据有:' + str(len(_list_all)) + '<br>'
        else:  # 区间积分
            u_min = userinfo_pd[userinfo_pd['integral'] >= min]
            u_max = userinfo_pd[userinfo_pd['integral'] <= max]
            on_cloums = self.get_pd_same_cloums(u_min.columns.values.tolist(), u_max.columns.values.tolist())
            pd_on = pd.merge(u_min, u_max, on=on_cloums)
            on_cloums = self.get_pd_same_cloums(pd_on.columns.values.tolist(), userinfo_pd.columns.values.tolist())
            _list_all = pd.merge(userinfo_pd, pd_on, on=on_cloums)
            logStr = logStr + '积分区间数据大于' + str(min) + '并且小于' + str(max) + '的数据有:' + str(len(_list_all)) + '<br>'

        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_integral_source(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        积分来源
        * author		: gg
        * creat date	: 2018/7/24 
        '''
        logStr = '当前计算是：积分来源<br>'

        if not params:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        sql = ''  # 拼接卡等级sql
        for params_value in params:
            tmp = "fromId=" + str(params_value) if not sql else " or fromId=" + str(params_value)
            sql = sql + tmp
        if not sql:
            logStr = logStr + 'err--拼装sql出错<br>'
            return self.empty_pd(), logStr
        else:
            sql_where = '(' + sql + ') and bid=' + str(bid)

        sql = 'select userId from online_userinfo_integral_log where ' + sql_where
        # sql查询出数据放到二维数组并且转换为pd对象
        ulist = self.db.execute(sql)
        count = ulist.rowcount
        logStr = logStr + '从积分log数据库中取出数据:' + str(count) + '<br>'
        # 查询数据后转换成list默认是一个迭代器cardLeve
        tmp = [[]] * count
        i = 0
        if ulist:
            for val in ulist:
                tmp2 = []
                tmp2.append(val.userId)
                tmp[i] = tmp2.copy()
                i = i + 1
        userinfo_pd = pd.DataFrame(tmp, columns=['userId'])
        logStr = logStr + '实例化到Pandas中数据有:' + str(len(userinfo_pd)) + '<br>'
        gp = userinfo_pd.groupby(by=['userId'])
        newdf = gp.size()  # 去重后数据
        pd_count = newdf.reset_index(name='count1')
        logStr = logStr + 'userId去重后活跃数据有:' + str(len(pd_count)) + '<br>'

        # 销毁变量 预防内存过高
        del ulist, tmp, tmp2, userinfo_pd, gp
        gc.collect()

        # 准备好所有用户数据
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'

        on_cloums = self.get_pd_same_cloums(userinfo_all_pd.columns.values.tolist(), pd_count.columns.values.tolist())
        _list_all = pd.merge(userinfo_all_pd, pd_count, on=on_cloums)
        logStr = logStr + '所有注册用户与积分来源数据交集后:' + str(len(_list_all)) + '<br>'

        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_coupons_use_yetai(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券使用频率/倾向--业态
        * author		: gg
        * creat date	: 2018/7/24 
        '''
        logStr = '当前计算是：卡券使用频率/倾向--业态<br>'
        funParams = {'statusType': 3}
        outArr, logStrTmp = self.coupons_yetai(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp
        return outArr, logStr

    def analysis_coupons_receive_yetai(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券领用频率/倾向--业态
        * author		: gg
        * creat date	: 2018/7/24 
        '''
        logStr = '当前计算是：卡券领用频率/倾向--业态<br>'
        funParams = {'statusType': 2}
        outArr, logStrTmp = self.coupons_yetai(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp
        return outArr, logStr

    def analysis_coupons_persistence_yetai(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券存留数量/倾向--业态
        * author		: gg
        * creat date	: 2018/7/24 
        '''
        logStr = '当前计算是：卡券存留数量/倾向--业态<br>'
        funParams = {'statusType': 999}
        outArr, logStrTmp = self.coupons_yetai(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp
        return outArr, logStr

    def analysis_coupons_overdue_yetai(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券过期数量/倾向--业态
        * author		: gg
        * creat date	: 2018/7/24 
        '''
        logStr = '当前计算是：卡券过期数量/倾向--业态<br>'
        funParams = {'statusType': 4}
        outArr, logStrTmp = self.coupons_yetai(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp
        return outArr, logStr

    def coupons_yetai(self, returnArr, bid, indate, params, otherParams, nodeParams, funParams):
        '''
        卡券--业态
        :return: 
        '''
        logStr = ''

        # 业态换取店铺id
        business_shops = base_mode('online_business_shops')
        sidArr = []
        dbSelect = self.db.query(business_shops.id, business_shops.yetaiId, ).filter(business_shops.bid == bid).all()
        for k, val in enumerate(dbSelect):
            if str(val[1]) in params:
                sidArr.append(val[0])
        if not sidArr:
            logStr = logStr + '通过业态没有找到店铺<br>'
            return self.empty_pd(), logStr
        logStr = logStr + '通过业态找到:' + str(len(sidArr)) + '个店铺<br>'
        a = 1
        evalArr = {'dayStr': '-1 week'}
        stime = self.get_str_to_stime(indate, evalArr)
        logStr = logStr + '默认查询一个星期的卡券操作记录<br>'
        if funParams['statusType'] == 999:  # 留存
            sqlwhere = 'bid=' + str(bid) + ' and indate>=' + str(stime)
        else:
            sqlwhere = 'bid=' + str(bid) + ' and indate>=' + str(stime) + ' and statusType=' + str(
                funParams['statusType'])
        sql = 'select userId,sid from online_coupons_user_action_log where ' + sqlwhere
        # sql查询出数据放到二维数组并且转换为pd对象
        ulist = self.db.execute(sql)
        count = ulist.rowcount

        logStr = logStr + '从卡券操作log数据库中取出数据:' + str(count) + '<br>'
        # 查询数据后转换成list默认是一个迭代器cardLeve
        tmp = [[]] * count
        i = 0
        if ulist:
            for val in ulist:
                tmp2 = []
                tmp2.append(val.userId)
                tmp2.append(val.sid)
                tmp[i] = tmp2.copy()
                i = i + 1
        userinfo_pd = pd.DataFrame(tmp, columns=['userId', 'sid'])
        logStr = logStr + '实例化到Pandas中数据有:' + str(len(userinfo_pd)) + '<br>'
        # 相当于sql中in
        _list = []
        for params_value in sidArr:
            _list.append(userinfo_pd[userinfo_pd['sid'] == params_value])
        userinfo_pd = pd.concat(_list, ignore_index=True)  # 两个pandas数据拼接
        logStr = logStr + '在选择的店铺业态中国的数据有:' + str(len(userinfo_pd)) + '<br>'

        gp = userinfo_pd.groupby(by=['userId'])
        newdf = gp.size()  # 去重后数据
        pd_count = newdf.reset_index(name='count1')
        logStr = logStr + 'userId去重后数据有:' + str(len(pd_count)) + '<br>'

        # 销毁变量 预防内存过高
        del ulist, tmp, userinfo_pd, gp
        gc.collect()

        # 准备好所有用户数据
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'

        on_cloums = self.get_pd_same_cloums(userinfo_all_pd.columns.values.tolist(), pd_count.columns.values.tolist())
        _list_all = pd.merge(userinfo_all_pd, pd_count, on=on_cloums)
        logStr = logStr + '所有注册用户与积分来源数据交集后:' + str(len(_list_all)) + '<br>'

        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_coupons_use_from(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券使用频率/倾向--来源
        * author		: gg
        * creat date	: 2018/7/26
        '''
        logStr = '当前计算是：卡券使用频率/倾向--来源<br>'
        funParams = {'statusType': 3}
        outArr, logStrTmp = self.coupons_from(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp
        return outArr, logStr

    def analysis_coupons_receive_from(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券领用频率/倾向--来源
        * author		: gg
        * creat date	: 2018/7/26
        '''
        logStr = '当前计算是：卡券领用频率/倾向--来源<br>'
        funParams = {'statusType': 2}
        outArr, logStrTmp = self.coupons_from(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp
        return outArr, logStr

    def analysis_coupons_persistence_from(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券存留数量/倾向--来源
        * author		: gg
        * creat date	: 2018/7/26
        '''
        logStr = '当前计算是：卡券存留数量/倾向--来源<br>'
        funParams = {'statusType': 999}
        outArr, logStrTmp = self.coupons_from(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp
        return outArr, logStr

    def analysis_coupons_overdue_from(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券过期数量/倾向--来源
        * author		: gg
        * creat date	: 2018/7/26
        '''
        logStr = '当前计算是：卡券过期数量/倾向--来源<br>'
        funParams = {'statusType': 4}
        outArr, logStrTmp = self.coupons_from(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp
        return outArr, logStr

    def coupons_from(self, returnArr, bid, indate, params, otherParams, nodeParams, funParams):
        '''
        卡券--来源
        :return: 
        '''
        logStr = ''

        evalArr = {'dayStr': '-1 week'}
        stime = self.get_str_to_stime(indate, evalArr)
        logStr = logStr + '默认查询一个星期的卡券操作记录<br>'
        if funParams['statusType'] == 999:  # 留存
            sqlwhere = 'bid=' + str(bid) + ' and indate>=' + str(stime)
        else:
            sqlwhere = 'bid=' + str(bid) + ' and indate>=' + str(stime) + ' and statusType=' + str(
                funParams['statusType'])
        sql = 'select userId,fromType from online_coupons_user_action_log where ' + sqlwhere
        # sql查询出数据放到二维数组并且转换为pd对象
        ulist = self.db.execute(sql)
        count = ulist.rowcount

        logStr = logStr + '从卡券操作log数据库中取出数据:' + str(count) + '<br>'
        # 查询数据后转换成list默认是一个迭代器cardLeve
        tmp = [[]] * count
        i = 0
        if ulist:
            for val in ulist:
                tmp2 = []
                tmp2.append(val.userId)
                tmp2.append(val.fromType)
                tmp[i] = tmp2.copy()
                i = i + 1
        userinfo_pd = pd.DataFrame(tmp, columns=['userId', 'fromType'])
        logStr = logStr + '实例化到Pandas中数据有:' + str(len(userinfo_pd)) + '<br>'
        # 相当于sql中in
        _list = []
        for params_value in params:
            _list.append(userinfo_pd[userinfo_pd['fromType'] == int(params_value)])
        userinfo_pd = pd.concat(_list, ignore_index=True)  # 两个pandas数据拼接
        logStr = logStr + '在选择的卡券来源的数据有:' + str(len(userinfo_pd)) + '<br>'

        gp = userinfo_pd.groupby(by=['userId'])
        newdf = gp.size()  # 去重后数据
        pd_count = newdf.reset_index(name='count1')
        logStr = logStr + 'userId去重后数据有:' + str(len(pd_count)) + '<br>'

        # 销毁变量 预防内存过高
        del ulist, tmp, userinfo_pd, gp
        gc.collect()

        # 准备好所有用户数据
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'

        on_cloums = self.get_pd_same_cloums(userinfo_all_pd.columns.values.tolist(), pd_count.columns.values.tolist())
        _list_all = pd.merge(userinfo_all_pd, pd_count, on=on_cloums)
        logStr = logStr + '所有注册用户与积分来源数据交集后:' + str(len(_list_all)) + '<br>'

        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_coupons_use_hot(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券使用频率/倾向--热度
        * author		: gg
        * creat date	: 2018/7/26
        '''
        logStr = '当前计算是：卡券使用频率/倾向--热度<br>'
        funParams = {'statusType': 3}
        outArr, logStrTmp = self.coupons_hot(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp

        return outArr, logStr

    def analysis_coupons_receive_hot(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券领用频率/倾向--热度
        * author		: gg
        * creat date	: 2018/7/26
        '''
        logStr = '当前计算是：卡券领用频率/倾向--热度<br>'
        funParams = {'statusType': 2}
        outArr, logStrTmp = self.coupons_hot(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp

        return outArr, logStr

    def analysis_coupons_persistence_hot(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券存留数量/倾向--热度
        * author		: gg
        * creat date	: 2018/7/26
        '''
        logStr = '当前计算是：卡券存留数量/倾向--热度<br>'
        funParams = {'statusType': 999}
        outArr, logStrTmp = self.coupons_hot(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp

        return outArr, logStr

    def analysis_coupons_overdue_hot(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券过期数量/倾向--热度
        * author		: gg
        * creat date	: 2018/7/26
        '''
        logStr = '当前计算是：卡券过期数量/倾向--热度<br>'
        funParams = {'statusType': 4}
        outArr, logStrTmp = self.coupons_hot(returnArr, bid, indate, params, otherParams, nodeParams, funParams)
        logStr = logStr + logStrTmp

        return outArr, logStr

    def coupons_hot(self, returnArr, bid, indate, params, otherParams, nodeParams, funParams):
        '''
        卡券频率/倾向--热度
        :return: 
        '''
        logStr = ''

        evalArr = {'dayStr': '-3 month'}
        stime = self.get_str_to_stime(indate, evalArr)
        logStr = logStr + '默认查询一个星期的卡券操作记录<br>'
        sqlwhere = 'bid=' + str(bid) + ' and indate>=' + str(stime)
        sql = 'select userId,fromType from online_coupons_user_action_log where ' + sqlwhere
        # sql查询出数据放到二维数组并且转换为pd对象
        ulist = self.db.execute(sql)
        count = ulist.rowcount

        logStr = logStr + '从卡券操作log数据库中取出数据:' + str(count) + '<br>'
        # 查询数据后转换成list默认是一个迭代器cardLeve
        tmp = [[]] * count
        i = 0
        if ulist:
            for val in ulist:
                tmp2 = []
                tmp2.append(val.userId)
                tmp2.append(val.fromType)
                tmp[i] = tmp2.copy()
                i = i + 1
        userinfo_pd = pd.DataFrame(tmp, columns=['userId', 'fromType'])
        logStr = logStr + '实例化到Pandas中数据有:' + str(len(userinfo_pd)) + '<br>'

        Analysis_hot_config = self.DefaultValues['Analysis_hot_config']

        min = Analysis_hot_config[int(params[0])]['valArr']['min']
        max = Analysis_hot_config[int(params[0])]['valArr']['max']
        # 准备好所有用户数据
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'
        if min == -1 and max == -1:  # 不用 频率热度是不用的话。不需要去计算热度
            pd_count = userinfo_pd.groupby(by=['userId']).size().reset_index(name='coupons_count')  # 去重后数据
            on_cloums = self.get_pd_same_cloums(userinfo_all_pd.columns.values.tolist(),
                                                pd_count.columns.values.tolist())
            outArr = pd.merge(userinfo_all_pd, pd_count, on=on_cloums)
        else:
            outArr, logStrTmp = self.compute_hot(userinfo_pd, funParams, bid)
            logStr = logStr + logStrTmp

        if min == -1 and max == -1:  # 取出商圈所有会员排除活跃会员

            logStr = logStr + '频率热度数据有:' + str(len(outArr)) + '<br>'
            userId_pd = pd.DataFrame(userinfo_all_pd['userId'], columns=['userId'])
            outArr = outArr.append(userId_pd)
            _list_all = outArr.drop_duplicates(subset=['userId'], keep=False)
            logStr = logStr + '频率热度数据和所有用户数据得出差集就是沉默不用用户数:' + str(len(_list_all)) + '<br>'
        elif max == -1:  # 频繁
            pd_max = outArr[outArr['hot'] > max]
            on_cloums = self.get_pd_same_cloums(pd_max.columns.values.tolist(), userinfo_all_pd.columns.values.tolist())
            _list_all = pd.merge(userinfo_all_pd, pd_max, on=on_cloums)
            logStr = logStr + '频率热度数据大于' + str(max) + '的数据有:' + str(len(_list_all)) + '<br>'
        else:  # 区间
            u_min = outArr[outArr['hot'] >= min]
            u_max = outArr[outArr['hot'] <= max]
            on_cloums = self.get_pd_same_cloums(u_min.columns.values.tolist(), u_max.columns.values.tolist())
            pd_on = pd.merge(u_min, u_max, on=on_cloums)
            on_cloums = self.get_pd_same_cloums(pd_on.columns.values.tolist(), userinfo_all_pd.columns.values.tolist())
            _list_all = pd.merge(userinfo_all_pd, pd_on, on=on_cloums)
            logStr = logStr + '频率热度数据大于' + str(min) + '并且小于' + str(max) + '的数据有:' + str(len(_list_all)) + '<br>'
        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def compute_hot(self, userinfo_pd, funParams, bid):
        '''
        计算热度
        :return: 
        '''
        logStr = ''
        # 先计算goupby userId,fromType的数据
        gpType = userinfo_pd.groupby(['userId', 'fromType'])
        newdfType = gpType.size()  # 去重后数据
        pd_count_type = newdfType.reset_index(name='countType')
        logStr = logStr + '先计算goupby userId,fromType的数据' + str(len(pd_count_type)) + '<br>'
        if funParams['statusType'] == 3:  # 使用热度  使用/领用
            logStr = logStr + '使用热度=使用/领用<br>'
            use_pd = pd_count_type[pd_count_type['fromType'] == 3]
            logStr = logStr + '卡券使用的数据' + str(len(use_pd)) + '<br>'
            get_pd = pd_count_type[pd_count_type['fromType'] == 2]
            logStr = logStr + '卡券领用的数据' + str(len(get_pd)) + '<br>'
            # new_pd=use_pd['countType']/get_pd['countType']
            new_pd = pd.merge(use_pd, get_pd, on=['userId'], suffixes=['_use', '_get'])
            logStr = logStr + '使用和领用数据交集后，计算热度，数据量：' + str(len(new_pd)) + '<br>'
            new_pd['hot'] = new_pd['countType_use'] / new_pd['countType_get']
            logStr = logStr + '计算热度后数据：' + str(len(new_pd)) + '<br>'
            del use_pd, get_pd, newdfType, pd_count_type
            gc.collect()
        elif funParams['statusType'] == 2:  # 领用热度    用户领用/用户注册时间开始的商圈发放的卡卷()领用多个算一个
            logStr = logStr + '领用热度=用户领用/用户注册时间开始的商圈发放的卡卷<br>'
            # 准备好所有用户数据
            sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
            sql = 'select id,intime from online_userinfo where ' + sql_where
            ulist = self.db.execute(sql)
            count = ulist.rowcount

            tmp = [[]] * count
            i = 0
            if ulist:
                for val in ulist:
                    tmp2 = []
                    tmp2.append(val.id)
                    tmp2.append(val.intime)
                    tmp[i] = tmp2.copy()
                    i = i + 1
            userinfo_all_pd = pd.DataFrame(tmp, columns=['userId', 'intime'])

            # 销毁变量 预防内存过高
            del ulist, tmp, tmp2
            gc.collect()
            logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'
            # 用户领用的数据
            get_pd = pd_count_type[pd_count_type['fromType'] == 2]
            logStr = logStr + '商圈的用户领用的数据有:' + str(len(get_pd)) + '<br>'
            on_cloums = self.get_pd_same_cloums(userinfo_all_pd.columns.values.tolist(), get_pd.columns.values.tolist())
            # 得到用户领用的数据+用户注册时间
            new_pd = pd.merge(userinfo_all_pd, get_pd, on=on_cloums)
            logStr = logStr + '商圈的用户数据和领用数据交集后:' + str(len(new_pd)) + '<br>'
            # 取出所有卡券
            sql_where = ' bid=' + str(bid) + ' and isDel=0'
            sql = 'select id,intime from online_coupons where ' + sql_where
            clist = self.db.execute(sql)
            count = clist.rowcount

            tmp = [[]] * count
            i = 0
            if clist:
                for val in clist:
                    tmp2 = []
                    tmp2.append(val.id)
                    tmp2.append(val.intime)
                    tmp[i] = tmp2.copy()
                    i = i + 1
            coupons_pd = pd.DataFrame(tmp, columns=['id', 'intime'])
            logStr = logStr + '所有卡券数据:' + str(len(coupons_pd)) + '<br>'

            # 计算大于卡券发放时间的个数--闭包函数
            def coupons_time(arr, c_pd):
                return len(c_pd[c_pd['intime'] >= arr['intime']])

            new_pd['intimecount'] = new_pd.apply(coupons_time, axis=1, c_pd=coupons_pd)
            logStr = logStr + '用户数据的注册时间以后的卡券数量数据:' + str(len(new_pd)) + '<br>'

            new_pd['hot'] = new_pd['countType'] / new_pd['intimecount']
            logStr = logStr + '计算热度后数据：' + str(len(new_pd)) + '<br>'
            del userinfo_all_pd, clist, coupons_pd
            gc.collect()
        elif funParams['statusType'] == 4:  # //过期  过期/领用
            logStr = logStr + '过期热度=过期/领用<br>'
            pass_pd = pd_count_type[pd_count_type['fromType'] == 4]
            logStr = logStr + '卡券过期的数据' + str(len(pass_pd)) + '<br>'
            get_pd = pd_count_type[pd_count_type['fromType'] == 2]
            logStr = logStr + '卡券领用的数据' + str(len(get_pd)) + '<br>'
            # new_pd=use_pd['countType']/get_pd['countType']
            new_pd = pd.merge(pass_pd, get_pd, on=['userId'], suffixes=['_pass', '_get'])
            logStr = logStr + '过期和领用数据交集后，计算热度，数据量：' + str(len(new_pd)) + '<br>'
            new_pd['hot'] = new_pd['countType_pass'] / new_pd['countType_get']
            logStr = logStr + '计算热度后数据：' + str(len(new_pd)) + '<br>'
            del pass_pd, get_pd, newdfType, pd_count_type
            gc.collect()
        elif funParams['statusType'] == 999:  # 留存  未使用/总领用
            logStr = logStr + '留存=未使用/总领用<br>'
            # 所有未使用的卡券
            sql_where = ' bid=' + str(bid) + ' and statusType=2'
            sql = 'select userId from online_coupons_user_list where ' + sql_where
            clist = self.db.execute(sql)
            count = clist.rowcount

            tmp = [[]] * count
            i = 0
            if clist:
                for val in clist:
                    tmp2 = []
                    tmp2.append(val.userId)
                    tmp[i] = tmp2.copy()
                    i = i + 1
            coupons_pd = pd.DataFrame(tmp, columns=['userId'])
            logStr = logStr + '所有未使用的卡券数据:' + str(len(coupons_pd)) + '<br>'
            pd_count = coupons_pd.groupby(by=['userId']).size().reset_index(name='coupons_count')  # 去重后数据
            logStr = logStr + '通过userId去重后数据:' + str(len(pd_count)) + '<br>'

            get_pd = pd_count_type[pd_count_type['fromType'] == 2]
            logStr = logStr + '卡券领用的数据' + str(len(get_pd)) + '<br>'
            new_pd = pd.merge(pd_count, get_pd, on=['userId'], suffixes=['_pd', '_get'])
            logStr = logStr + '过期和领用数据交集后，计算热度，数据量：' + str(len(new_pd)) + '<br>'
            new_pd['hot'] = new_pd['coupons_count_pd'] / new_pd['countType_get']
            logStr = logStr + '计算热度后数据：' + str(len(new_pd)) + '<br>'
        else:
            logStr = logStr + 'statusType参数错误<br>'
            new_pd = self.empty_pd()
        return new_pd, logStr

    def analysis_active_weixin(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        微信活跃
        * author		: gg
        * creat date	: 2018/7/29
        '''
        # self.write(otherParams.otherParams)
        logStr = '当前计算是：微信活跃<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        stime = self.get_str_to_stime(indate, evalArr)
        sql_where = 'bid=' + str(bid) + ' and userType=2 and  indate>=' + str(stime)

        sql = 'select uid from online_behavior_list where ' + sql_where
        clist = self.db_conduct.execute(sql)
        count = clist.rowcount

        tmp = [[]] * count
        i = 0
        if clist:
            for val in clist:
                tmp2 = []
                tmp2.append(val.uid)
                tmp[i] = tmp2.copy()
                i = i + 1
        behavior_pd = pd.DataFrame(tmp, columns=['uid'])
        logStr = logStr + '所有活跃数据:' + str(len(behavior_pd)) + '<br>'
        pd_count = behavior_pd.groupby(by=['uid']).size().reset_index(name='behavior_count')  # 去重后数据
        logStr = logStr + '通过uid去重后数据:' + str(len(pd_count)) + '<br>'

        min = evalArr['min']
        max = evalArr['max']
        # 准备好所有用户数据
        sql_where = ' bid=' + str(bid) + ' and subscribe=1 and userId>0'
        userinfo_weixin_all_pd = self.sql_userinfo_weixin_pd(sql_where)
        logStr = logStr + '所有已经关注的数据:' + str(len(userinfo_weixin_all_pd)) + '<br>'

        if min == -1 and max == -1:  # 沉默：最近无任何积分变动

            # 取两个pd的差集，左边是全数组，append是最好是一个列的数据，不然多出来的咧会有NaN，不好处理了


            logStr = logStr + '活跃数据有:' + str(len(pd_count)) + '<br>'
            userId_pd = pd.DataFrame(pd_count['uid'], columns=['uid'])
            userinfo_weixin_all_pd = userinfo_weixin_all_pd.append(userId_pd)
            _list_all = userinfo_weixin_all_pd.drop_duplicates(subset=['uid'], keep=False)
            logStr = logStr + '活跃微信用户数据和所有微信用户数据得出差集就是沉默不用用户数:' + str(len(_list_all)) + '<br>'
        elif max == -1:  # 高：最近有5日+产生积分变动
            pd_max = pd_count[pd_count['behavior_count'] > max]
            on_cloums = self.get_pd_same_cloums(pd_max.columns.values.tolist(),
                                                userinfo_weixin_all_pd.columns.values.tolist())
            _list_all = pd.merge(userinfo_weixin_all_pd, pd_max, on=on_cloums)
            logStr = logStr + '活跃度数据大于' + str(max) + '的数据有:' + str(len(_list_all)) + '<br>'
        else:  # 区间
            u_min = pd_count[pd_count['behavior_count'] >= min]
            u_max = pd_count[pd_count['behavior_count'] <= max]
            on_cloums = self.get_pd_same_cloums(u_min.columns.values.tolist(), u_max.columns.values.tolist())
            pd_on = pd.merge(u_min, u_max, on=on_cloums)
            on_cloums = self.get_pd_same_cloums(pd_on.columns.values.tolist(),
                                                userinfo_weixin_all_pd.columns.values.tolist())
            _list_all = pd.merge(userinfo_weixin_all_pd, pd_on, on=on_cloums)
            logStr = logStr + '活跃度数据大于' + str(min) + '并且小于' + str(max) + '的数据有:' + str(len(_list_all)) + '<br>'

        # 准备好所有用户数据

        _list_all = _list_all.drop(columns=['uid'])  # 删除微信的uid，合并后会变columns--uid_x
        print(_list_all)
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'
        _list_all = pd.merge(_list_all, userinfo_all_pd, on=['userId'])
        print(_list_all)
        logStr = logStr + '微信数据于用户数据交集后:' + str(len(_list_all)) + '<br>'
        a = 1

        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_active_component(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        组件--活跃 沉默、低：1次/组件、中2-5次/组件、高5+次/组件
        跟微信活跃一样就是一个sqlwhere不同
        * author		: gg
        * creat date	: 2018/7/29
        '''
        # self.write(otherParams.otherParams)
        logStr = '当前计算是：组件--活跃<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        stime = self.get_str_to_stime(indate, evalArr)
        # sql_where = 'bid='+str(bid)+' and userType=2 and  indate>='+str(stime)
        sql_where = 'bid=' + str(bid) + ' and userType=2   and (typeId=6 or typeId=7 or typeId=10) and  indate>=' + str(
            stime)

        sql = 'select uid from online_behavior_list where ' + sql_where
        clist = self.db_conduct.execute(sql)
        count = clist.rowcount

        tmp = [[]] * count
        i = 0
        if clist:
            for val in clist:
                tmp2 = []
                tmp2.append(val.uid)
                tmp[i] = tmp2.copy()
                i = i + 1
        behavior_pd = pd.DataFrame(tmp, columns=['uid'])
        logStr = logStr + '所有活跃数据:' + str(len(behavior_pd)) + '<br>'
        pd_count = behavior_pd.groupby(by=['uid']).size().reset_index(name='behavior_count')  # 去重后数据
        logStr = logStr + '通过uid去重后数据:' + str(len(pd_count)) + '<br>'

        min = evalArr['min']
        max = evalArr['max']
        # 准备好所有微信用户数据
        sql_where = ' bid=' + str(bid) + ' and subscribe=1 and userId>0'
        userinfo_weixin_all_pd = self.sql_userinfo_weixin_pd(sql_where)
        logStr = logStr + '所有已经关注的数据:' + str(len(userinfo_weixin_all_pd)) + '<br>'

        if min == -1 and max == -1:  # 沉默：最近无任何积分变动

            # 取两个pd的差集，左边是全数组，append是最好是一个列的数据，不然多出来的咧会有NaN，不好处理了


            logStr = logStr + '活跃数据有:' + str(len(pd_count)) + '<br>'
            userId_pd = pd.DataFrame(pd_count['uid'], columns=['uid'])
            userinfo_weixin_all_pd = userinfo_weixin_all_pd.append(userId_pd)
            _list_all = userinfo_weixin_all_pd.drop_duplicates(subset=['uid'], keep=False)
            logStr = logStr + '活跃微信用户数据和所有微信用户数据得出差集就是沉默不用用户数:' + str(len(_list_all)) + '<br>'
        elif max == -1:  # 高：最近有5日+产生积分变动
            pd_max = pd_count[pd_count['behavior_count'] > max]
            on_cloums = self.get_pd_same_cloums(pd_max.columns.values.tolist(),
                                                userinfo_weixin_all_pd.columns.values.tolist())
            _list_all = pd.merge(userinfo_weixin_all_pd, pd_max, on=on_cloums)
            logStr = logStr + '活跃度数据大于' + str(max) + '的数据有:' + str(len(_list_all)) + '<br>'
        else:  # 区间
            u_min = pd_count[pd_count['behavior_count'] >= min]
            u_max = pd_count[pd_count['behavior_count'] <= max]
            on_cloums = self.get_pd_same_cloums(u_min.columns.values.tolist(), u_max.columns.values.tolist())
            pd_on = pd.merge(u_min, u_max, on=on_cloums)
            on_cloums = self.get_pd_same_cloums(pd_on.columns.values.tolist(),
                                                userinfo_weixin_all_pd.columns.values.tolist())
            _list_all = pd.merge(userinfo_weixin_all_pd, pd_on, on=on_cloums)
            logStr = logStr + '活跃度数据大于' + str(min) + '并且小于' + str(max) + '的数据有:' + str(len(_list_all)) + '<br>'

        # 准备好所有用户数据

        _list_all = _list_all.drop(columns=['uid'])  # 删除微信的uid，合并后会变columns--uid_x

        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'
        _list_all = pd.merge(_list_all, userinfo_all_pd, on=['userId'])
        logStr = logStr + '微信数据于用户数据交集后:' + str(len(_list_all)) + '<br>'

        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
            # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_component_active_date(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        组件--活跃 日期 区间
        * author		: gg
        * creat date	: 2018/7/29
        '''
        # self.write(otherParams.otherParams)
        logStr = '当前计算是：组件--活跃 日期 区间<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        stime = self.get_str_to_stime(indate, evalArr)
        # sql_where = 'bid='+str(bid)+' and userType=2 and  indate>='+str(stime)
        sql_where = 'bid=' + str(bid) + ' and userType=2 and (typeId=6 or typeId=7 or typeId=10)  and  indate>=' + str(
            stime)
        # and (typeId=6 or typeId=7 or typeId=10)
        sql = 'select uid from online_behavior_list where ' + sql_where
        clist = self.db_conduct.execute(sql)
        count = clist.rowcount

        tmp = [[]] * count
        i = 0
        if clist:
            for val in clist:
                tmp2 = []
                tmp2.append(val.uid)
                tmp[i] = tmp2.copy()
                i = i + 1
        behavior_pd = pd.DataFrame(tmp, columns=['uid'])
        logStr = logStr + '所有活跃数据:' + str(len(behavior_pd)) + '<br>'
        pd_count = behavior_pd.groupby(by=['uid']).size().reset_index(name='behavior_count')  # 去重后数据
        logStr = logStr + '通过uid去重后数据:' + str(len(pd_count)) + '<br>'

        # 准备好所有微信用户数据
        sql_where = ' bid=' + str(bid) + ' and subscribe=1 and userId>0'
        userinfo_weixin_all_pd = self.sql_userinfo_weixin_pd(sql_where)
        logStr = logStr + '所有已经关注的数据:' + str(len(userinfo_weixin_all_pd)) + '<br>'

        on_cloums = self.get_pd_same_cloums(pd_count.columns.values.tolist(),
                                            userinfo_weixin_all_pd.columns.values.tolist())
        _list_all = pd.merge(userinfo_weixin_all_pd, pd_count, on=on_cloums)
        logStr = logStr + '活跃数据于微信用户数据交集后数据有:' + str(len(_list_all)) + '<br>'



        # 准备好所有用户数据

        _list_all = _list_all.drop(columns=['uid'])  # 删除微信的uid，合并后会变columns--uid_x
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'
        _list_all = pd.merge(_list_all, userinfo_all_pd, on=['userId'])
        logStr = logStr + '微信数据于用户数据交集后:' + str(len(_list_all)) + '<br>'

        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_component_active_time(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        时间
上午：6点-11点半、中午：11点半-2点半、下午：14点半-17点半、晚餐:17点半-19点半、晚上:19点半-23点半、深夜:23点半-6点
        * author		: gg
        * creat date	: 2018/7/29
        '''
        # self.write(otherParams.otherParams)
        logStr = '当前计算是：组件--活跃 时间段<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        evalArr['dayStr'] = '-1 week'
        if 'dateCate' in nodeParams:
            if nodeParams['dateCate'] != 0:
                analysis_list = base_mode('online_analysis_list')
                dfind = self.db.query(analysis_list).filter(analysis_list.id == nodeParams['dateCate']).first()
                if hasattr(dfind, 'id'):
                    evalCate = self.evalOtherParams(dfind.otherParams)
                    if not evalCate:
                        evalArr['dayStr'] = evalCate['dayStr']


        stime = self.get_str_to_stime(indate, evalArr)
        otherSql=''
        for i in range(stime, indate, 86400):
            if evalArr['jiayi'] == '1':
                tmpETime = indate + 86400
            else:
                tmpETime = indate
            startTime = int(time.mktime(
                time.strptime(time.strftime("%Y-%m-%d", time.localtime(i)) + ' ' + evalArr['min'] + ':00',
                              "%Y-%m-%d %H:%M:%S")))
            endTime = int(time.mktime(
                time.strptime(time.strftime("%Y-%m-%d", time.localtime(tmpETime)) + ' ' + evalArr['max'] + ':00',
                              "%Y-%m-%d %H:%M:%S")))
            otherTmp = '(intime>='+str(startTime)+' and intime<='+str(endTime)+')'
            if otherSql=='':
                otherSql=otherTmp
            else:
                otherSql = otherSql+' or '+otherTmp
            a = 1
        if otherSql !='':
            otherSql=' and '+otherSql
        sql_where = 'bid=' + str(bid) + ' and userType=2 and (typeId=6 or typeId=7 or typeId=10) ' + otherSql
        #and (typeId=6 or typeId=7 or typeId=10)
        # and (typeId=6 or typeId=7 or typeId=10)
        sql = 'select uid from online_behavior_list where ' + sql_where
        clist = self.db_conduct.execute(sql)
        count = clist.rowcount

        tmp = [[]] * count
        i = 0
        if clist:
            for val in clist:
                tmp2 = []
                tmp2.append(val.uid)
                tmp[i] = tmp2.copy()
                i = i + 1
        behavior_pd = pd.DataFrame(tmp, columns=['uid'])
        logStr = logStr + '所有活跃数据:' + str(len(behavior_pd)) + '<br>'
        pd_count = behavior_pd.groupby(by=['uid']).size().reset_index(name='behavior_count')  # 去重后数据
        logStr = logStr + '通过uid去重后数据:' + str(len(pd_count)) + '<br>'

        # 准备好所有微信用户数据
        sql_where = ' bid=' + str(bid) + ' and subscribe=1 and userId>0'
        userinfo_weixin_all_pd = self.sql_userinfo_weixin_pd(sql_where)
        logStr = logStr + '所有已经关注的数据:' + str(len(userinfo_weixin_all_pd)) + '<br>'

        on_cloums = self.get_pd_same_cloums(pd_count.columns.values.tolist(),
                                            userinfo_weixin_all_pd.columns.values.tolist())
        _list_all = pd.merge(userinfo_weixin_all_pd, pd_count, on=on_cloums)
        logStr = logStr + '活跃数据于微信用户数据交集后数据有:' + str(len(_list_all)) + '<br>'

        # 准备好所有用户数据
        sql_where = ' bid=' + str(bid) + ' and subscribe=1 and userId>0'
        userinfo_weixin_all_pd = self.sql_userinfo_weixin_pd(sql_where)
        logStr = logStr + '所有已经关注的数据:' + str(len(userinfo_weixin_all_pd)) + '<br>'

        # 准备好所有用户数据

        _list_all = _list_all.drop(columns=['uid'])  # 删除微信的uid，合并后会变columns--uid_x
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'
        _list_all = pd.merge(_list_all, userinfo_all_pd, on=['userId'])
        logStr = logStr + '微信数据于用户数据交集后:' + str(len(_list_all)) + '<br>'

        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_component_coupons_name(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        卡券内容名称//没有初始化数据
        * author		: gg
        * creat date	: 2018/7/29
        '''
        # self.write(otherParams.otherParams)
        logStr = '当前计算是：卡券内容名称<br>'
        # 所有未使用的卡券
        sql_where = ' bid=' + str(bid) + ' and cardType=4'
        sql = 'select userId from online_coupons_user_list where ' + sql_where
        clist = self.db.execute(sql)
        count = clist.rowcount

        tmp = [[]] * count
        i = 0
        if clist:
            for val in clist:
                tmp2 = []
                tmp2.append(val.userId)
                tmp[i] = tmp2.copy()
                i = i + 1
        coupons_pd = pd.DataFrame(tmp, columns=['userId'])
        logStr = logStr + '所有组件的卡券数据:' + str(len(coupons_pd)) + '<br>'
        pd_count = coupons_pd.groupby(by=['userId']).size().reset_index(name='coupons_count')  # 去重后数据
        logStr = logStr + '通过userId去重后数据:' + str(len(pd_count)) + '<br>'

        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'
        _list_all = pd.merge(pd_count, userinfo_all_pd, on=['userId'])
        logStr = logStr + '组件的卡券数据于用户数据交集后:' + str(len(_list_all)) + '<br>'


        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def _user_active(self, returnArr, bid, indate, params, otherParams, nodeParams,evalArr):
        '''
        计算活跃数据
        :param returnArr: 
        :param bid: 
        :param indate: 
        :param params: 
        :param otherParams: 
        :param nodeParams: 
        :param evalArr: 
        :return: 
        '''
        logStr=''
        stime = self.get_str_to_stime(indate, evalArr)

        sql_where = 'bid=' + str(bid) + ' and indate >=' + str(stime)
        sql = 'select uid,userType from online_behavior_list where ' + sql_where
        clist = self.db_conduct.execute(sql)
        count = clist.rowcount

        tmp = [[]] * count
        i = 0
        if clist:
            for val in clist:
                tmp2 = []
                tmp2.append(val.uid)
                tmp2.append(val.userType)
                tmp[i] = tmp2.copy()
                i = i + 1
        behavior_pd = pd.DataFrame(tmp, columns=['uid', 'userType'])
        logStr = logStr + '所有活跃数据:' + str(len(behavior_pd)) + '<br>'
        pd_count = behavior_pd.groupby(by=['uid', 'userType']).size().reset_index(name='behavior_count')  # 去重后数据
        logStr = logStr + '通过uid去重后数据:' + str(len(pd_count)) + '<br>'

        if 'countStart' in evalArr:#活跃等级
            pd_count = pd_count[pd_count['behavior_count'] >= int(evalArr['countStart'])]
            logStr = logStr + '活跃等级大于'+str(evalArr['countStart'])+'的数据有:' + str(len(pd_count)) + '<br>'


        pd_weixin = pd_count[pd_count['userType'] == 2]
        pd_user = pd_count[pd_count['userType'] == 1]
        pd_user = pd_user.drop(columns=['userType'])  # 删除userType，后面有重复

        logStr = logStr + '所有微信数据:' + str(len(pd_weixin)) + '<br>'

        # 准备好所有微信用户数据
        sql_where = ' bid=' + str(bid) + ' and subscribe=1 and userId>0'
        userinfo_weixin_all_pd = self.sql_userinfo_weixin_pd(sql_where)

        logStr = logStr + '所有已经关注微信的数据:' + str(len(userinfo_weixin_all_pd)) + '<br>'
        pd_weixin = pd.merge(userinfo_weixin_all_pd, pd_weixin, on=['uid'])
        logStr = logStr + '活跃数据于微信用户数据交集后数据有:' + str(len(pd_weixin)) + '<br>'

        # 准备好所有用户数据
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)

        pd_weixin = pd_weixin.drop(columns=['uid'])  # 删除微信的uid，合并后会变columns--uid_x
        logStr = logStr + '活跃注册用户数据有:' + str(len(pd_user)) + '<br>'
        pd_user['userId'] = pd_user['uid']
        pd_user = pd_user.drop(columns=['uid'])
        pd_user = pd.merge(pd_user, userinfo_all_pd, on=['userId'])
        logStr = logStr + '活跃注册用户于所有注册用户交集数据有:' + str(len(pd_user)) + '<br>'
        # 合并之前需要剔除不要的列--都转成注册用户数据了。所以不需要userType
        pd_user = pd_user.drop(columns=['behavior_count', 'uid', 'userPhone', 'infoData', 'userType'])
        pd_weixin = pd_weixin.drop(columns=['behavior_count', 'userType'])

        _list_all = pd.concat([pd_weixin, pd_user], ignore_index=True)
        logStr = logStr + '用户数据和用户数据合并后数据有:' + str(len(_list_all)) + '<br>'

        logStr = logStr + '商圈的所有注册用户数据有:' + str(len(userinfo_all_pd)) + '<br>'
        _list_all = pd.merge(userinfo_all_pd, _list_all, on=['userId'])
        logStr = logStr + '活跃数据于用户数据交集后:' + str(len(_list_all)) + '<br>'


        if 'not_active' in evalArr:#非活跃区间
            # 取两个pd的差集，左边是全数组，append是最好是一个列的数据，不然多出来的咧会有NaN，不好处理了
            userId_pd = pd.DataFrame(_list_all['userId'], columns=['userId'])
            userinfo_all_pd = userinfo_all_pd.append(userId_pd)
            _list_all = userinfo_all_pd.drop_duplicates(subset=['userId'], keep=False)
            logStr = logStr + '活跃区间数据和所有用户数据得出差集就是非活跃区间用户数:' + str(len(_list_all)) + '<br>'


        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr

    def analysis_user_active(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        活跃
        * author		: gg
        * creat date	: 2018/7/30
        '''
        logStr = '当前计算是：活跃<br>'
        evalArr={}
        evalArr['dayStr'] = '-3 month'
        _list_all, logStrTmp = self._user_active( returnArr, bid, indate, params, otherParams, nodeParams,evalArr)
        logStr = logStr + logStrTmp
        return _list_all, logStr

    def analysis_user_active_between(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        活跃区间
        * author		: gg
        * creat date	: 2018/7/30
        '''
        logStr = '当前计算是：活跃区间<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        _list_all, logStrTmp = self._user_active( returnArr, bid, indate, params, otherParams, nodeParams,evalArr)
        logStr = logStr + logStrTmp
        return _list_all, logStr
    def analysis_user_not_active_between(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        非活跃区间
        * author		: gg
        * creat date	: 2018/7/30
        '''
        logStr = '当前计算是：非活跃区间<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        evalArr['not_active']=1#标识非活跃区间
        _list_all, logStrTmp = self._user_active( returnArr, bid, indate, params, otherParams, nodeParams,evalArr)
        logStr = logStr + logStrTmp
        return _list_all, logStr

    def analysis_user_active_level(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        活跃等级
        * author		: gg
        * creat date	: 2018/7/30
        '''
        logStr = '当前计算是：活跃等级<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        evalArr['dayStr'] = '-3 month'
        _list_all, logStrTmp = self._user_active( returnArr, bid, indate, params, otherParams, nodeParams,evalArr)
        logStr = logStr + logStrTmp
        return _list_all, logStr

    def analysis_park_date(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        时间区间--停车场
        * author		: gg
        * creat date	: 2018/7/30
        '''
        logStr = '当前计算是：时间区间--停车场<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        # 时间字符串转换成时间
        stime = self.get_str_to_stime(indate, evalArr)
        sql_where = 'bid=' + str(bid) + ' and indate>=' + str(stime)
        sql = 'select wxId as uid from online_business_park_list where ' + sql_where
        logStr = logStr + '取数据时间：' + time.strftime("%Y-%m-%d", time.localtime(stime)) + '<br>'
        # sql查询出数据放到二维数组并且转换为pd对象
        ulist = self.db.execute(sql)
        count = ulist.rowcount
        logStr = logStr + '从数据库中取出数据:' + str(count) + '<br>'
        # 查询数据后转换成list默认是一个迭代器cardLeve
        tmp = [[]] * count
        i = 0
        if ulist:
            for val in ulist:
                tmp2 = []
                tmp2.append(val.uid)
                tmp[i] = tmp2.copy()
                i = i + 1

        userinfo_pd = pd.DataFrame(tmp, columns=['uid'])
        logStr = logStr + '实例化到Pandas中数据有:' + str(len(userinfo_pd)) + '<br>'
        gp = userinfo_pd.groupby(by=['uid'])
        newdf = gp.size()  # 去重后数据
        pd_count = newdf.reset_index(name='count1')
        logStr = logStr + 'uid去重后活跃数据有:' + str(len(pd_count)) + '<br>'

        # 准备好所有微信用户数据
        sql_where = ' bid=' + str(bid) + ' and subscribe=1 and userId>0'
        userinfo_weixin_all_pd = self.sql_userinfo_weixin_pd(sql_where)

        logStr = logStr + '所有已经关注微信的数据:' + str(len(userinfo_weixin_all_pd)) + '<br>'
        pd_weixin = pd.merge(userinfo_weixin_all_pd, userinfo_pd, on=['uid'])
        logStr = logStr + '停车场数据于微信用户数据交集后数据有:' + str(len(pd_weixin)) + '<br>'

        # 准备好所有用户数据
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '所有用户数据:' + str(len(userinfo_all_pd)) + '<br>'
        pd_weixin = pd_weixin.drop(columns=['uid'])
        _list_all = pd.merge(pd_weixin, userinfo_all_pd, on=['userId'])

        logStr = logStr + '停车场数据于用户数据交集后:' + str(len(_list_all)) + '<br>'
        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr


    def analysis_pos_date(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        时间区间--pos
        * author		: gg
        * creat date	: 2018/7/30
        '''
        logStr = '当前计算是：时间区间--pos<br>'
        evalArr = self.evalOtherParams(otherParams.otherParams)
        if not evalArr:
            logStr = logStr + 'err--参数错误<br>'
            return self.empty_pd(), logStr
        # 时间字符串转换成时间
        stime = self.get_str_to_stime(indate, evalArr)
        sql_where = 'bid=' + str(bid) + ' and indate>=' + str(stime)
        sql = 'select userId from online_order_list where ' + sql_where
        logStr = logStr + '取数据时间：' + time.strftime("%Y-%m-%d", time.localtime(stime)) + '<br>'
        # sql查询出数据放到二维数组并且转换为pd对象
        ulist = self.db.execute(sql)
        count = ulist.rowcount
        logStr = logStr + '从数据库中取出数据:' + str(count) + '<br>'
        # 查询数据后转换成list默认是一个迭代器cardLeve
        tmp = [[]] * count
        i = 0
        if ulist:
            for val in ulist:
                tmp2 = []
                tmp2.append(val.userId)
                tmp[i] = tmp2.copy()
                i = i + 1

        userinfo_pd = pd.DataFrame(tmp, columns=['userId'])
        logStr = logStr + '实例化到Pandas中数据有:' + str(len(userinfo_pd)) + '<br>'
        gp = userinfo_pd.groupby(by=['userId'])
        newdf = gp.size()  # 去重后数据
        pd_count = newdf.reset_index(name='count1')
        logStr = logStr + 'userId去重后活跃数据有:' + str(len(pd_count)) + '<br>'
        # 准备好所有用户数据
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '所有用户数据:' + str(len(userinfo_all_pd)) + '<br>'
        _list_all = pd.merge(userinfo_all_pd, pd_count, on=['userId'])
        logStr = logStr + 'pos数据于用户数据交集后:' + str(len(_list_all)) + '<br>'
        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr


    def analysis_pos_shops(self, returnArr, bid, indate, params, otherParams, nodeParams):
        '''
        店铺--pos
        * author		: gg
        * creat date	: 2018/7/30
        '''
        logStr = '当前计算是：店铺--pos<br>'


        sql_where = 'bid=' + str(bid)
        sql = 'select userId,sid from online_order_list where ' + sql_where
        a=1
        # sql查询出数据放到二维数组并且转换为pd对象
        ulist = self.db.execute(sql)
        count = ulist.rowcount
        logStr = logStr + '从数据库中取出数据:' + str(count) + '<br>'
        # 查询数据后转换成list默认是一个迭代器cardLeve
        tmp = [[]] * count
        i = 0
        if ulist:
            for val in ulist:
                tmp2 = []
                tmp2.append(val.userId)
                tmp2.append(val.sid)
                tmp[i] = tmp2.copy()
                i = i + 1

        userinfo_pd = pd.DataFrame(tmp, columns=['userId','sid'])
        logStr = logStr + '实例化到Pandas中数据有:' + str(len(userinfo_pd)) + '<br>'
        # 相当于sql中in
        _list = []
        for params_value in params:
            _list.append(userinfo_pd[userinfo_pd['sid'] == int(params_value)])
        userinfo_pd = pd.concat(_list, ignore_index=True)  # 两个pandas数据拼接
        logStr = logStr + '在选择的店铺pos的数据有:' + str(len(userinfo_pd)) + '<br>'


        gp = userinfo_pd.groupby(by=['userId'])
        newdf = gp.size()  # 去重后数据
        pd_count = newdf.reset_index(name='count1')
        logStr = logStr + 'userId去重后活跃数据有:' + str(len(pd_count)) + '<br>'
        # 准备好所有用户数据
        sql_where = ' bid=' + str(bid) + ' and userPhone<>0'
        userinfo_all_pd = self.sql_userinfo_pd(sql_where)
        logStr = logStr + '所有用户数据:' + str(len(userinfo_all_pd)) + '<br>'
        _list_all = pd.merge(userinfo_all_pd, pd_count, on=['userId'])
        logStr = logStr + 'pos数据于用户数据交集后:' + str(len(_list_all)) + '<br>'
        if nodeParams['isfirst'] == 1:  # 如果没有传数据进来自己组织
            logStr = logStr + '节点的第一个，返回自己的数据:' + str(len(_list_all)) + '<br>'
            return _list_all, logStr
        else:  # 从上一个数据里传过来数据
            logStr = logStr + '从上一个节点传过来数据:' + str(len(returnArr)) + '<br>'
            if returnArr.empty:
                logStr = logStr + '从上一个节点传过来数据为空，返回空<br>'
                return self.empty_pd(), logStr
            if _list_all.empty:
                logStr = logStr + '本身节点数据为空，返回空<br>'
                return self.empty_pd(), logStr
                # 两个的交集通过userId
        on_cloums = self.get_pd_same_cloums(_list_all.columns.values.tolist(), returnArr.columns.values.tolist())
        result = pd.merge(_list_all, returnArr, on=on_cloums)
        logStr = logStr + '从上一个节点传过来数据交集后:' + str(len(result)) + '<br>'
        return result, logStr