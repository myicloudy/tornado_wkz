# coding:utf-8

from Controller.Analysis.CommonAnalysis import CommonAnalysis
from Tool.Analysis.analysis import WoxAnalysis
from Model.shopmall_conduct.online_behavior_list import online_behavior_list as behavior
from Model.shopmall_main_db.online_analysis_user_list import online_analysis_user_list as user_list
from Model.shopmall_main_db.online_analysis_list import online_analysis_list as analysis_list
from Model.shopmall_main_db.base_model import base_mode
from Model.shopmall_analysis.base_model_analysis import base_mode_analysis
import gc
import pandas as pd
import time


isTest = True  # 是否测试状态


# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class Index(CommonAnalysis):
    def get(self):

        dodate = int(time.mktime(time.strptime(time.strftime('%Y-%m-%d', time.localtime()), '%Y-%m-%d')))
        # 取出用户提交的
        alist = self.db.query(user_list).filter(user_list.isDel == 0,user_list.dodate <= dodate).all()
        # alist = self.db.query(user_list).filter(user_list.isDel == 0).all()
        # 有日期的节点分类
        Analysis_data_list = self.DefaultValues['Analysis_data_list']
        dateCateListFind = self.db.query(analysis_list.id).filter(analysis_list.cate_id.in_(Analysis_data_list)).all()
        dateCateList = []
        for val in dateCateListFind:
            dateCateList.append(val[0])
        btime = dodate - 86400
        etime = dodate
        for i in range(btime, etime, 86400):
            indate = i
            for alist_value in alist:

                funcStime=time.time()#开始时间
                bid = alist_value.bid
                logStrs = '当前计算日期：' + time.strftime("%Y-%m-%d", time.localtime(indate)) + '<br>'
                logStrs = logStrs+'当前时间：' + time.strftime("%Y-%m-%d %H:%I:%S", time.localtime(time.time())) + '<br>'
                logStrs = logStrs + '当前bid：' + str(bid) + '<br>'
                logStrs = logStrs + '当前分析器id：' + str(alist_value.id) + '<br>'
                logStrs = logStrs + '计算分析器名称：' + alist_value.title + '<br>'
                self.checkTableName(bid)
                nodeIds = alist_value.nodeIds
                nodeIdsArr = nodeIds.split(',')
                returnArr = {}
                nodeOutArr = {}
                dateCate = 0;  # 日期分类,找出有日期参数的，如果没有回取默认值
                # 吧数据翻转，同样的节点内的数据统一执行

                for nodeIdsArr_value in nodeIdsArr:
                    nodeidArr = nodeIdsArr_value.split('_')
                    if nodeidArr[0] not in nodeOutArr:
                        nodeOutArr[nodeidArr[0]] = []
                    nodeOutArr[nodeidArr[0]].append(str(nodeidArr[1]))
                    if int(nodeidArr[0]) in dateCateList:
                        dateCate = nodeidArr[0]

                # self.write(nodeOutArr)
                firstii = 0
                nodeParams = {'dateCate': dateCate}
                returnArr = {}
                logStrs = logStrs + '#####start#####循环字方法分析数据##########<br>'
                logStrs = logStrs + '********************************<br>'
                for k in nodeOutArr:
                    # 查询每个节点的配置值
                    aconfigFind = self.db.query(analysis_list).filter(analysis_list.id == k).first()
                    if not hasattr(aconfigFind, 'id'):
                        logStrs = logStrs + 'err--没有找到当前节点<br>'
                        self.updateIndate(alist_value.id, i)#更新下次执行时间
                        continue
                    logStrs = logStrs + '计算当前的节点名称:' + aconfigFind.title + '<br>'
                    if firstii == 0:
                        nodeParams['isfirst'] = 1
                    else:
                        nodeParams['isfirst'] = 0
                    firstii = 1
                    params = nodeOutArr[k]  # 必须是list数据
                    # returnArr = getattr(AnalysisFunctions, 'a1')(self,'33')


                    try:
                        # 调用类执行里面方法，传入参数
                        a = 1
                        tmp = 'self.' + aconfigFind.functionName + "(returnArr, bid, indate,params, aconfigFind, nodeParams)"
                        returnArr,funcLogStrs = eval(tmp)

                        # returnArr, funcLogStrs=self.analysis_coupons_use_yetai(returnArr, bid, indate,params, aconfigFind, nodeParams)
                        logStrs = logStrs +funcLogStrs
                        logStrs = logStrs + aconfigFind.functionName+'方法返回来数据条数:' + str(len(returnArr)) + '<br>'
                        # logStrs = logStrs +  '子方法返回来数据条数:' + str(len(returnArr)) + '<br>'
                        a = 1
                    except Exception as e:
                        logStrs = logStrs + 'err--子方法执行错误<br>'+str(e)
                        returnArr=self.empty_pd()

                        self.AppLogging.warning('err--的函数执行失败:异常%s' % aconfigFind.functionName,str(e))
                        self.AppLogging.warning(str(e))
                        if isTest:
                            raise e
                    logStrs = logStrs + '********************************<br>'
                logStrs = logStrs + '#####end#####循环字方法分析数据##########<br>'
                logStrs = logStrs + '所有子方法执行后返回来数据条数:' + str(len(returnArr)) + '<br>'
                # funcLogStrs=''
                # logStrs = logStrs + funcLogStrs
                # print('returnArr==',len(returnArr))
                # return
                if returnArr.empty:
                    logStrs = logStrs + '所有计算完事后返回数据是空'
                    logStrs = logStrs + '总计执行时间:' + str(time.time() - funcStime) + '秒<br>'
                    self.AppLogging.info(logStrs)
                    self.updateIndate(alist_value.id, i)  # 更新下次执行时间
                    continue
                # 返回的数据要去计算我所要得到的数据
                # 分片数据
                userType1Arr = returnArr[returnArr['userType'] == 1]#注册用户数据
                userType2Arr = returnArr[returnArr['userType'] == 2]#微信用户

                logStrs = logStrs + '注册用户数据:' + str(len(userType1Arr)) + '<br>'
                logStrs = logStrs + '微信用户数据:' + str(len(userType2Arr)) + '<br>'

                # return
                if not userType1Arr.empty:#注册用户数据
                    userinfo_weixin = base_mode('online_userinfo_weixin')
                    ulist_wx = self.db.query(userinfo_weixin.userId, userinfo_weixin.openid).filter(
                        userinfo_weixin.bid == bid, userinfo_weixin.openid != None).all()
                    ulist_wx_pd = pd.DataFrame(ulist_wx, columns=['userId', 'openid'])
                    out1=pd.merge(userType1Arr,ulist_wx_pd,on='userId')
                    del ulist_wx_pd
                else:
                    out1=self.empty_pd()
                logStrs = logStrs + '注册用户数据其中有openid的数据有:' + str(len(out1)) + '<br>'

                if not userType2Arr.empty:
                    online_userinfo = base_mode('online_userinfo')
                    ulist = self.db.query(online_userinfo.id, online_userinfo.userPhone,
                                          online_userinfo.age).filter(online_userinfo.bid == bid,
                                                                      online_userinfo.userPhone != 0).all()

                    ulist_pd = pd.DataFrame(ulist, columns=['uid', 'userPhone', 'age'])
                    out2 = pd.merge(userType2Arr, ulist_pd, on='uid')
                    del ulist
                else:
                    out2 = self.empty_pd()
                logStrs = logStrs + '微信数据其中有用户基础数据的有:' + str(len(out2)) + '<br>'

                out = pd.concat([out1, out2], ignore_index=True)  # 两个pandas数据拼接
                logStrs = logStrs + '微信数据和用户基础数据拼接后:' + str(len(out)) + '<br>'
                # print(out)
                # return


                # 销毁变量 预防内存过高
                del out1, out2
                gc.collect()
                tmpall = []
                if not out.empty:
                    #先删除已经存在的数据
                    aid=alist_value.id
                    online_analysis_list=base_mode_analysis('online_analysis_list_'+str(bid))
                    self.db_analysis.query(online_analysis_list).filter(online_analysis_list.indate==indate,online_analysis_list.aid==aid).delete()
                    self.db_analysis.commit()

                    for index, row in out.iterrows():
                        tmp={}
                        tmp['intime'] = int(time.time())
                        tmp['indate'] =indate
                        tmp['aid']=aid
                        tmp['userType']=row['userType']
                        tmp['uid'] = row['uid']
                        tmp['userId'] = row['userId']
                        tmp['openId'] = row['openid']
                        tmp['userPhone'] = row['userPhone']
                        tmpall.append(tmp)
                    # print(tmpall)
                    # return
                    try:
                        self.db_analysis.execute(online_analysis_list.__table__.insert(), tmpall)
                        self.db_analysis.commit()
                    except:
                        self.AppLogging.warning("分析器out写入数据库错误%s" % tmp)
                logStrs = logStrs + '写入数据库有:' + str(len(tmpall)) + '<br>'
                logStrs = logStrs + '总计执行时间:' + str(time.time()-funcStime) + '秒<br>'
                self.AppLogging.warning(logStrs)
                self.updateIndate(alist_value.id,i)
        self.AppLogging.info('执行完毕')
        self.write('ok')

    def updateIndate(self,id,indate):
        '''
        更新下次执行时间
        :param id: 
        :param indate: 
        :return: 
        '''

        try:
            self.db.query(user_list).filter(user_list.id == id).update(
                {'dodate': indate + 86400+86400}, synchronize_session=False)
            self.db.commit()
        except:
            self.AppLogging.info("更新时间错误")

    def checkTableName(self, bid):
        '''
        检查表是否存在，不存在就创建
        :return: 
        '''
        sqlStr = "CREATE TABLE `online_analysis_list_" + str(bid) + "` (\
                  `id` bigint(20) NOT NULL AUTO_INCREMENT,\
                  `aid` bigint(20) DEFAULT '0',\
                  `userType` tinyint(4) DEFAULT '0',\
                  `uid` bigint(20) DEFAULT '0',\
                  `userId` bigint(20) DEFAULT '0',\
                  `openId` varchar(50) DEFAULT '',\
                  `userPhone` decimal(20,0) DEFAULT '0',\
                  `intime` int(11) DEFAULT '0',\
                  `indate` int(11) DEFAULT '0',\
                  PRIMARY KEY (`id`),\
                  KEY `NewIndex1` (`uid`),\
                  KEY `NewIndex2` (`indate`),\
                  KEY `userId` (`userId`)\
                ) ENGINE=InnoDB AUTO_INCREMENT=1 DEFAULT CHARSET=utf8mb4 ROW_FORMAT=DYNAMIC"
        try:
            self.db_analysis.execute(sqlStr)
            self.db_analysis.commit()
        except:
            self.AppLogging.info('bid:%s的表已经存在' % str(bid))
        pass

    def fuc(self, t):
        a = 1
        return t

        # #27 100W 22S
        # dbFind=self.db_conduct.query(behavior.userType,behavior.uid).filter(behavior.bid==86).all()
        # # print(len(dbFind))
        #
        # self.write('活跃数据量')
        # df=pd.DataFrame(dbFind,columns=['userType','uid'])
        # self.write(str(len(df)))
        # gp=df.groupby(by=['uid'])
        # newdf = gp.size()  # 去重后数据
        # ss=newdf.reset_index(name='count1')
        # self.write('<br>######<br>')
        # self.write('计算活跃次数后：')
        # self.write(str(len(ss)))
        # self.write('<br>######<br>')
        # self.write('index of CommonAnalysis')
