# coding:utf-8

from Controller.Test.CommonTest import CommonTest
import logging
import logging.config
# 加载前面的标准配置
from Config.logging import LOGGING
import time

logging.config.dictConfig(LOGGING)
import pandas as pd
from Model.shopmall_main_db.base_model import base_mode
from Model.shopmall_conduct.base_model_conduct import base_mode_conduct
import datetime
import csv

import hashlib
from Tool.Idcrypt import Idcrypt


# import pydf
# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg

class Index(CommonTest):
    def get(self):
        import secrets
        token = secrets.token_urlsafe()
        self.write(token)
        scr = Idcrypt()
        # scr.encrypt(8965856)
        print('加密密文：', scr.encrypt(10000001511321151216612103))
        print('解密结果：', scr.decrypt(scr.encrypt(10000001511321151216612103)))

        # slist = [scr.encrypt(ch) for ch in range(10000,9999999)]
        # print(len(slist))
        # print(len(set(slist)))
        #     print('++++')
        #     a = scr.encrypt(ch)
        #     for ch2 in range(1, ch):
        #         if a == scr.encrypt(ch2):
        #             print(ch2)
        #             print('重复')
        #             continue
        print('----------------')

        # scr.decrypt(scr.encrypt(1000))
        # print('加密密文：', scr.encrypt(1000))
        # print('解密结果：', scr.decrypt(scr.encrypt(1000)))

        # x = 13
        # K = 0b00101010100101001001011000101101
        # print (len('0b00111010100101001101011000111111'))
        # print('初始明文：', format(x, '016b'))
        # print('加密密文：', scr.encrypt(K, x))
        # print('解密结果：', scr.decrypt(K, scr.encrypt(K, x)))
        # for i in range(1,9999):
        #     # print('加密密文：', scr.encrypt( i))
        #     # print('解密结果：', scr.decrypt( scr.encrypt( i)))
        #     if i != scr.decrypt( scr.encrypt( i)):
        #         print('error')
        # # key = 'ZDFKJMNX'
        # data = '12'
        # print("秘钥：" + key)
        #
        # print ("明文：" + data)
        #
        #
        # jiami_key, jiemi_key = self.suanfa(key)
        # miwen = self.bianma(jiami_key, data)
        # mingwen = self.bianma(jiemi_key, miwen)
        #
        # print("加密明文所得的密文：" + miwen)
        #
        # print("解密密文所得的明文：" + mingwen)

        # print(self.get_hash_key('11'))
        # print(self.get_num(88888888))
        # self.AppLogging.warning('ssss')
        # m2 = hashlib.md5()
        # stime=time.time()
        # sdb=base_mode('online_coupons_user_action_log')
        # slist=self.db.query(sdb.bid,sdb.userId,sdb.couponsId).all()
        #
        # # sdb = base_mode_conduct('online_behavior_list')
        # # slist = self.db_conduct.query(sdb.userType, sdb.uid, sdb.bid).all()
        #
        # csvfile = open('csv.csv', 'w')  # 打开方式还可以使用file对象
        # writer = csv.writer(csvfile)
        # writer.writerow(['姓名', '年龄', '电话'])
        #
        # # slist = [
        # #     ('小河', '25', '1234567'),
        # #     ('小芳', '18', '789456')
        # # ]
        # writer.writerows(slist)
        # self.write(str(time.time()-stime))
        # csvfile.close()

        # Analysis_hot_config = self.DefaultValues['Analysis_hot_config']
        # print(Analysis_hot_config)
        # print(Analysis_hot_config[1]['valArr'])
        # data_b = [[1,11],[10,101]]
        # data_a = [[1, 2], [1, 2], [1, 1], [1, 1], [1, 1]]
        # a = pd.DataFrame(data_a,columns=['userId','userType'])
        # #已知上面的Pandas对象
        # #求出userType=1出现的次数除以userType=2出现的次数，返回pandas对象
        # # a['userId']=a['userId']*2
        # def sd(arr,dd):
        #     print(dd)
        #     return arr['userType']+1
        # a['new']=a.apply(sd,axis=1,dd=data_b)
        # print(a)
        # b = pd.DataFrame(data_b,columns=['userId','userType'])
        # a = a.append(b)
        # # a = a.append(b)
        # result = a.drop_duplicates(subset=['userId'], keep=False)
        # print (result)
        # indate=1532275200
        # indate_timestamp = datetime.datetime.fromtimestamp(indate)
        # last_month = indate_timestamp.month - 1
        # last_year = indate_timestamp.year
        # if last_month == 0:
        #     last_month = 12
        #     last_year -= 1
        # month_time = datetime.datetime(month=last_month, year=last_year, day=indate_timestamp.day)
        # print (month_time.strftime("%Y-%m-%d %H:%S:%M"))
        # print(int(time.mktime(month_time.timetuple())))

        # ll = [[22, 1302782213, 1, 22, 1], [23, 1324976952, 1, 23, 1]]
        # df = pd.DataFrame(ll, columns=['uid','userPhone','userType','userId','infoData'])
        # aa=[[22, 1302782213, 1, 22, 1], [26, 1324976952, 1, 26, 1]]
        # df2 = pd.DataFrame(aa, columns=['uid', 'userPhone','userType', 'userId', 'infoData'])
        # #取出两个pd的相同列明
        # df_clouns=df.columns.values.tolist()
        # df2_clouns = df2.columns.values.tolist()
        # out_cloums=[]
        # for val in df_clouns:
        #     if val in df2_clouns:
        #         out_cloums.append(val)
        # print(out_cloums)
        #
        #
        # tmp=pd.merge(df, df2, on=['userId'])
        # print(tmp)
        # cols_to_use = df2.columns - df.columns
        # dfNew = pd.merge(df, df2[cols_to_use], left_index=True, right_index=True, how='outer')
        # print(dfNew)
        return

        # bid=86
        #
        # tmp = [(237825, 1, 237825, 1), (233294, 1, 233294, 1)]
        # returnArr = pd.DataFrame(tmp, columns=['userId', 'userType', 'uid', 'infoData'])
        #
        # online_userinfo = base_mode('online_userinfo')
        # ulist = self.db.query(online_userinfo.id, online_userinfo.userPhone,
        #                       online_userinfo.age).filter(online_userinfo.bid == bid).all()
        #
        # ulist_pd = pd.DataFrame(ulist, columns=['userId', 'userPhone', 'age'])
        # sd=pd.merge(returnArr,ulist_pd,on='userId')
        # print(sd)
        # return
        # df=pd.DataFrame([(1,2,3,4)],columns=['uid','userType','infodate','userId'])
        # print(df.columns.values.tolist())
        # if 'userType' in df.columns.values.tolist():
        #     print('hehe')
        # return
        # userinfo_member_card = base_mode('online_userinfo_member_card')
        # sql='userinfo_member_card.bid == bid,userinfo_member_card.bid == bid'
        # card_cate_list = self.db.query(userinfo_member_card).filter(userinfo_member_card.status == 1).all()
        # for val in card_cate_list:
        #     self.write(val.title)
        # sql='select id,title from online_userinfo_member_card'
        # carlist=self.db.execute(sql)
        # for val in carlist:
        #     self.write(val.title)
        #     self.write(val.intime)
        # print(carlist)
        # return

        # df1 = pd.DataFrame([[1, 2, 3], [4, 5, 6], [3, 9, 0], [8, 0, 3]], columns=['x1', 'x2', 'x3'])
        # df2 = pd.DataFrame([[1, 2], [4, 6], [3, 9]], columns=['x1', 'x4'])
        # print(df1)
        # print(df2)
        # df3 = pd.merge(df1, df2, how='left', on='x1')
        # print('df3')
        # print(df3)
        # df4 = pd.merge(df1, df2, how='right', on='x1')
        # print('df4')
        # print(df4)
        # df5 = pd.merge(df1, df2, how='inner', on='x1')
        # print('df5')
        # print(df5)
        # df6 = pd.merge(df1, df2, how='outer', on='x1')
        # print('df6')
        # print(df6)

        # aaa = pd.DataFrame({'A': [1, 4, 4, 4, 1, 6], 'B': [2, 12, 2, 12, 12, 12]})
        # ptaaa = aaa.pivot_table(aaa, index='A',values=['C'], aggfunc=len)
        # print(ptaaa)
        # print(ptaaa['B'][1])

        # dblist=self.db.query(online_districts.CID,online_districts.DistrictName,online_districts.id).all()
        # df=pd.DataFrame(dblist,columns=['CID','DistrictName','id'])#序列化到pandas里
        # gp=df.groupby(by=['CID'])
        # newdf=gp.size()#去重后数据
        # ss=newdf.reset_index(name='count1')#去重后进入数组
        # aa=df.drop_duplicates(['CID'])
        # print(aa)
        # print('#####################')
        #
        # print(ss)
        # print('#####################')
        # # print(ss)
        # a=pd.merge(aa,ss,on='CID',how='inner')
        # print(a)
        # print('#####################')
        # b=pd.merge(aa,df,on='CID',how='inner')
        # print(b)
        pass

    def check_xsrf_cookie(self):
        pass

    def post(self, *args, **kwargs):
        self.write('ok')
