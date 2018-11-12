# coding:utf-8
# @Explain : 说明
# @Time    : 2017/12/19 下午7:52
# @Author  : gg
# @FileName: CronSendMessage.py

import time
import json
from Controller.WeixinUpdate.CommonWeixinUpdate import CommonWeixinUpdate
from Model.shopmall_main_db.online_business_kf_message_list import online_business_kf_message_list
from Model.shopmall_main_db.online_business_message_send_config import online_business_message_send_config
from Model.shopmall_main_db.online_business_message_send_list_log import online_business_message_send_list_log
from Model.shopmall_main_db.online_business_message_send_list import online_business_message_send_list
from Model.shopmall_main_db.online_business_message_send_statistics import online_business_message_send_statistics
from Model.shopmall_main_db.online_message_template import online_message_template
from Model.shopmall_main_db.online_config_weixin_template import online_config_weixin_template
from Model.shopmall_main_db.online_message_log import online_message_log
from Model.shopmall_main_db.online_message_list import online_message_list
from Model.shopmall_main_db.online_userinfo_ext import online_userinfo_ext
from Model.shopmall_main_db.online_birthday_coupons_package_log import online_birthday_coupons_package_log

from urllib.parse import urlencode
import urllib
from Model.shopmall_main_db.online_business_weixin_config import online_business_weixin_config
import re
from urllib.parse import unquote
import html

kefu_list_db = online_business_kf_message_list
list_log_db = online_business_message_send_list_log
send_list_db = online_business_message_send_list

# 消息列队-扩展字段、及回调 类型配置
config_send_list_extension_type = {'1': {'valId': 1, 'valame': '后台推送', 'callback': ''},
                                   '2': {'valId': 2, 'valame': '商圈卡券即将到期通知', 'callback': ''},
                                   '3': {'valId': 3, 'valame': '商圈卡券到期通知', 'callback': ''},
                                   '4': {'valId': 4, 'valame': '商圈卡券推送', 'callback': 'woxCouponsPushCallback'},
                                   '5': {'valId': 5, 'valame': '微信卡券推送', 'callback': 'weixinCouponsPushCallback'},
                                   '6': {'valId': 6, 'valame': '卡包推送', 'callback': 'couponsPackagePushCallback'}
                                   }
from tornado import gen
from tornado.ioloop import IOLoop
from tornado.queues import Queue
import time
from datetime import timedelta

# guoguo de openid    o7uimuMFtOIQCttPmT-zLInYy5S0

import aiohttp
import asyncio

mx = 500  # 线程数
q = asyncio.Queue()
#测试取不同token
isTest=0

# @Explain : 入口方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class Index(CommonWeixinUpdate):
    def get(self):
        self.max_tries = 2
        intime = int(time.time())
        # sendList = self.db.query(send_list_db).filter(send_list_db.errCount < 3, send_list_db.sendTime < intime,
        #                                               send_list_db.send_type == 1, send_list_db.doitId == 6358).all()
        sendList = self.db.query(send_list_db).filter(send_list_db.errCount < 3, send_list_db.sendTime < intime).limit(
            500).all()
        self.AppLogging.warning(sendList)
        sendAll = {}
        for kk, sendArr in enumerate(sendList):
            fieldList = {'id', 'bid', 'extensionType', 'send_type', 'doitId', 'userId', 'uid', 'orderId0', 'orderId1',
                         'sendStatus', 'errCount', 'remark', 'sendTime', 'sendMessParams', 'templateJson', 'smsJson',
                         'intime', 'uptime', 'indate'}
            sendDlist = self.serializationTable(sendArr, fieldList)

            # 取得客服消息s模板信息
            sendDlist['messageJson'] = self.getMessageListKefuJsonString(sendDlist['doitId'])
            sendDlist['errCount'] = sendDlist['errCount'] + 1
            k = int(kk)
            sendAll[k] = sendDlist
        a=1
        self.loop = asyncio.get_event_loop()  # 事件循环
        self.ClientSession = aiohttp.ClientSession(loop=self.loop)  # aiohttp的session，get地址数据对象

        a = 1
        for x in sendAll:
            q.put_nowait(sendAll[x])

        print(q)

        # loop = asyncio.get_event_loop()

        tasks = [
            asyncio.async(self.do_work('task%d' % tk, q)) for tk in range(mx)]

        # loop.run_until_complete(asyncio.wait(tasks))
        asyncio.wait(tasks)

        self.ClientSession.close()
        # loop.close()



        self.write('ok')

    async def do_work(self, task_name, work_queue):
        while not work_queue.empty():
            queue_item = await work_queue.get()
            print('{0} grabbed item: {1}'.format(task_name, queue_item))
            # await asyncio.sleep(1)
            await self.hanld(queue_item)

    async def hanld(self, val):
        result = await self.executeSend(val)
        a = 1
        if result == 'ok':
            # 删除此数据
            # pass
            try:
                self.db.query(send_list_db).filter(send_list_db.id == val['id']).delete()
                self.db.commit()
            except:
                self.AppLogging.warning("-----删除发送列表出错-------")
                # print('-----删除发送列表出错-------')
        else:
            # 更新出错次数
            dblist = {send_list_db.errCount: send_list_db.errCount + 1}
            try:
                self.db.query(send_list_db).filter(send_list_db.id == val['id']).update(dblist,
                                                                                        synchronize_session=False)
                self.db.commit()
            except:
                self.AppLogging.warning("-----更新发送次数出错-------")
            pass

    async def producer(self, val):
        for item in val:
            await q.put(val[item])
            # print('Put %s' % val[item])

    # @Explain : 序列化表成Dlist
    # @Time    : 2017/12/28
    # @Author  : gg
    def serializationTable(self, obj, fieldList):
        dlist = {}
        for field in fieldList:
            dlist[field] = eval('obj.' + field)
        return dlist

    # @Explain : 发送微信客服消息
    # @Time    : 2017/12/28
    # @Author  : gg
    async def sendWeixinKfMessage(self, parameter):
        logs = {}
        try:
            sendMessParams = json.loads(parameter['sendMessParams'])
        except:#发送参有问题返回错误
            return None
        if parameter['errCount'] == 3:  # 短信消息补发
            logs['log_type'] = 3
            # 将用户参数传递过去
            parameter['sendMessParams'] = sendMessParams
            # 调用短信发送
            return self.publicSendSMS(parameter)


        elif parameter['errCount'] == 2:  # 模板消息补发
            logs['log_type'] = 2  # 微信模板消息补发日志

            # 组装消息格式
            templateJson = parameter['templateJson']
            try:
                templateJson = json.loads(templateJson)
            except:#模板数据有问题返回错误
                return None

            # 执行发送
            returnArr = await self.sendWxMessage(sendMessParams['openId'], parameter['bid'], templateJson['templateId'],
                                                 templateJson['params'], 1, templateJson['toUrl'])

            returnArr['errcode'] = returnArr['status']
            # true_part if condition else false_part  三元运算符
            returnArr['errmsg'] = returnArr['msg'] if 'msg' in returnArr  else returnArr['info']
            if returnArr['errcode'] == 0:
                sendStatus = 2
                # 修改 成功人数
                dblist = {kefu_list_db.sendSuccess: kefu_list_db.sendSuccess + 1}
                try:
                    self.db.query(kefu_list_db).filter(kefu_list_db.id == parameter['doitId']).update(dblist,
                                                                                                      synchronize_session=False)
                    self.db.commit()

                    # // 执行消息成功回调
                    self.sendSuccessCallback(parameter['extensionType'], parameter)
                except:
                    self.AppLogging.warning("-----更新发送成功数据 error-------")
                    # print('-----更新发送成功数据 error-------')
            else:
                sendStatus = 1
                dblist = {kefu_list_db.sendError: kefu_list_db.sendError + 1}
                try:
                    self.db.query(kefu_list_db).filter(kefu_list_db.id == parameter['doitId']).update(dblist,
                                                                                                      synchronize_session=False)
                    self.db.commit()
                except:
                    self.AppLogging.warning("-----更新发送失败数据 error-------")
                    # print('-----更新发送失败数据 error-------')

            # 写入日志
            logs['bid'] = parameter['bid']
            logs['extensionType'] = parameter['extensionType']  # 扩展配置
            logs['send_type'] = parameter['send_type']
            logs['doitId'] = parameter['doitId']
            logs['listId'] = parameter['id']
            logs['userId'] = parameter['userId']
            logs['uid'] = parameter['uid']
            logs['orderId0'] = parameter['orderId0']
            logs['orderId1'] = parameter['orderId1']
            logs['sendStatus'] = sendStatus
            logs['errcode'] = returnArr['errcode']
            logs['errmsg'] = returnArr['errmsg']
            logs['sendMessParams'] = parameter['sendMessParams']
            logs['templateJson'] = parameter['templateJson']
            # $logs['smsJson'] = $parameter['smsJson'];
            logs['remark'] = parameter['remark']
            logs['returnJson'] = json.dumps(returnArr)

            # 写入日志
            self.addLog(logs)


        else:  # 微信客服消息
            logs['log_type'] = 1  # 微信客服消息日志
            messageJson = parameter['messageJson']
            # 组装消息格式
            try:
                messageJson = json.loads(messageJson)
            except:#参数消息错误就不执行了
                return None
            messageJson['touser'] = sendMessParams['openId']

            # 判断传递了 toUrl 、一般不是每个用户一个动态url不要传递
            if 'toUrl' in sendMessParams:
                # 处理新闻类型的toUrl
                if messageJson['msgtype'] == 'news':
                    articles = messageJson['news']['articles']
                    toUrl = sendMessParams['toUrl']
                    for kk, vvalue in enumerate(articles):
                        if vvalue['url'] == '<{$toUrl}>':
                            vvalue['url'] = urllib.parse.quote_plus(toUrl)

                            pass
                # 处理放在文本的 toUrl
                elif messageJson['msgtype'] == 'text':  # 没有测试到后面测试
                    # print('ssstuoUrl---', sendMessParams['toUrl'])
                    messageJson['text']['content'] = re.sub(r'(?i)<{\$toUrl}>', sendMessParams['toUrl'],
                                                            messageJson['text']['content'], flags=re.IGNORECASE)
                    pass

            # 执行发送
            returnArr = await self.kf_send_message_to_weixin(parameter['bid'], messageJson)
            if returnArr['errmsg'] == 'ok':  # 发送成功
                sendStatus = 2

                dblist = {kefu_list_db.sendSuccess: kefu_list_db.sendSuccess + 1}
                try:
                    self.db.query(kefu_list_db).filter(kefu_list_db.id == parameter['doitId']).update(dblist,
                                                                                                      synchronize_session=False)
                    self.db.commit()

                    # // 执行消息成功回调
                    self.sendSuccessCallback(parameter['extensionType'], parameter)
                except:
                    self.AppLogging.warning("-----更新发送成功数据 error-------")
                    # print('-----更新发送成功数据 error-------')

            else:
                sendStatus = 1
                dblist = {kefu_list_db.sendError: kefu_list_db.sendError + 1}
                try:
                    self.db.query(kefu_list_db).filter(kefu_list_db.id == parameter['doitId']).update(dblist,
                                                                                                      synchronize_session=False)
                    self.db.commit()
                except:
                    self.AppLogging.warning("-----更新发送失败数据 error-------")
                    # print('-----更新发送失败数据 error-------')
                logs['orderId1'] = parameter['id']  # 失败的传递 队列id
            # 写入日志
            logs['bid'] = parameter['bid']
            logs['extensionType'] = parameter['extensionType']  # 扩展配置
            logs['send_type'] = parameter['send_type']
            logs['doitId'] = parameter['doitId']
            logs['listId'] = parameter['id']
            logs['userId'] = parameter['userId']
            logs['uid'] = parameter['uid']
            logs['orderId0'] = parameter['orderId0']
            logs['orderId1'] = parameter['orderId1']
            logs['sendStatus'] = sendStatus
            logs['errcode'] = returnArr['errcode']
            logs['errmsg'] = returnArr['errmsg']
            logs['sendMessParams'] = parameter['sendMessParams']
            # logs['templateJson'] = $parameter['templateJson'];
            # logs['smsJson'] = $parameter['smsJson'];
            logs['remark'] = parameter['remark']
            logs['returnJson'] = json.dumps(returnArr)
            self.addLog(logs)
        if sendStatus == 2:  # 成功
            return 'ok'
        else:
            return None

    # @Explain : 短信发送
    # @Time    : 2018、01、09
    # @Author  : gg
    def publicSendSMS(self, parameter):
        # 暂时不做任何补发操作、因为没模板、有合适模板再短信通知
        return None

    # @Explain : 微信发送模板
    # @Time    : 2017/12/29
    # @Author  : gg
    async def sendWxMessage(self, openId, bid, tempId, msg, fromType, tourl):
        if openId is None:
            return None
        templateInfo = self.db.query(online_message_template).filter(online_message_template.bid == bid,
                                                                     online_message_template.sendtype == 2,
                                                                     online_message_template.weixinId == tempId).first()
        if not templateInfo:
            return {'info': '发送失败', 'status': -1, 'msg': '没有相关模版'}
        else:
            sendContent = ''
            config_wt = online_config_weixin_template
            weixinTempInfo = self.db.query(config_wt).filter(config_wt.id == templateInfo.weixinTemplateId).first()
            if weixinTempInfo is None:
                return {'info': '发送失败', 'status': -1, 'msg': '没有微信默认配置模板'}
            fieldList = weixinTempInfo.fieldList
            # print(weixinTempInfo)
            fieldListArr = fieldList.split(',')  # 取出微信模版字段然后格式化
            params = {}
            if len(msg) != len(fieldListArr):
                return {'info': '发送失败', 'status': -1, 'msg': '所传内容字段和模板不符'}
            else:
                for key, val in enumerate(fieldListArr):
                    if bid == 86:  # 万菱汇模板消息要黑色字体、
                        params[val] = {msg[key], '#000000'}
                        sendContent += msg[key] + "|"
                        pass
                    else:
                        params[val] = msg[key]
            if tourl:
                params['tourl'] = tourl

            returnArr = await self.sendWeixinTemplateMessage(openId, bid, templateInfo.weixinTemplateId, params)
            if returnArr['errcode'] == 0:
                # 给message_list表插入发送内容
                # $message_list = M('message_list');
                data = {}  # 组织数据添加
                data['bid'] = bid
                data['valueType'] = 3  # //1文本格式;2html;3模版
                data['fromType'] = fromType  # 注册
                data['sendType'] = 2  # 2微信
                data['openId'] = openId
                if bid == 86:  # 万菱汇模板消息要黑色字体
                    data['sendContent'] = sendContent.strip('|')  # 发送微信通知内容
                else:
                    data['sendContent'] = '|'.join(params)  # 发送微信通知内容

                data['errorCode'] = returnArr['errcode']
                data['errorMsg'] = returnArr['errmsg']
                data['sendTime'] = int(time.time())  # 发送时间
                data['intime'] = int(time.time())
                data['indate'] = int(
                    time.mktime(time.strptime(time.strftime('%Y-%m-%d', time.localtime()), '%Y-%m-%d')))

                try:
                    dbreturn = self.db.execute(online_message_list.__table__.insert(), data)
                    self.db.commit()
                    self.AppLogging.info('online_message_log,insetr to db %s', data)
                except:
                    # print('----db insert online_message_log error-----')
                    self.AppLogging.warning('----db insert online_message_log error-----')
                if dbreturn is not None:
                    lastId = dbreturn.lastrowid
                else:
                    lastId = None
                # lastId=session.query(func.max(User.tid)).one()[0]
                # $lastId = $message_list->add($data);
                if lastId is not None:  # 在message_log表插入信息
                    # $message_log = M('message_log');
                    obj = {}
                    obj['mlistId'] = lastId
                    obj['bid'] = bid
                    obj['valueType'] = 3  # 1文本格式;2html;3模版
                    obj['fromType'] = fromType  # 注册
                    obj['sendType'] = 2  # 2微信
                    obj['errorCode'] = data['errorCode']
                    obj['errorMsg'] = data['errorMsg']
                    obj['intime'] = int(time.time())
                    obj['indate'] = int(
                        time.mktime(time.strptime(time.strftime('%Y-%m-%d', time.localtime()), '%Y-%m-%d')))

                    try:
                        rr = self.db.execute(online_message_log.__table__.insert(), obj)
                        self.db.commit()
                        self.AppLogging.info('online_message_log,insetr to db %s', obj)
                    except:
                        self.AppLogging.warning('----db insert online_message_log error-----')
                        # print('----db insert online_message_log error-----')

                return {'info': '发送成功', 'status': returnArr['errcode']}
            else:
                return {'info': '发送失败', 'status': returnArr['errcode'], 'msg': returnArr['errmsg']}

        pass

    # @Explain : 发送微信模板消息
    # @Time    : 2017/12/29
    # @Author  : gg
    async def sendWeixinTemplateMessage(self, openId, bid, weixinTemplateId, params):
        wxClass = self.Initialize_class_weixin(bid)
        # 传过来的参数通过微信不同的模板进行组装数据
        sendArr = self.WeixinTemplateMessageAssembly(openId, bid, weixinTemplateId, params)
        ret = {}
        if sendArr is None:
            ret['errcode'] = '-1'
            ret['errmsg'] = '系统内部错误'
            return ret
        sendReturn = await self.message_template_send(wxClass.AccessToken, sendArr)
        print('message_template_send  retrun', sendReturn)
        if sendReturn == None:
            self.AppLogging.error('发送微信模板消息错误')
            raise
        jsonArr = json.loads(sendReturn)
        return jsonArr
        # return wxClass.message_template_send(sendArr)

    # 发送微信模板消息
    # author     : gg
    # create date    : 2018-01-05
    async def message_template_send(self, token, sendContent):
        url = 'https://api.weixin.qq.com/cgi-bin/message/template/send?access_token=' + token

        tries = 0
        data = json.dumps(sendContent)
        end_data = unquote(data)
        end_data = html.unescape(end_data)
        a=1
        while tries < self.max_tries:  # 取不到数据会重试N次
            try:
                response = await self.ClientSession.post(
                    url, data=end_data)
                break
            except aiohttp.ClientError:
                pass
            tries += 1
        try:
            text = await response.text()  # 异步接收返回数据

            print(len(text))
            return text
        finally:
            await response.release()  # 异步释放资源

    # @Explain : 发送微信模板消息
    # @Time    : 2017/12/29
    # @Author  : gg
    def WeixinTemplateMessageAssembly(self, openId, bid, weixinTemplateId, params):
        weixinIdFind = self.db.query(online_message_template).filter(online_message_template.bid == bid,
                                                                     online_message_template.weixinTemplateId == weixinTemplateId).first()
        if not weixinIdFind:  # 微信那边的模板id错误。或者不存在
            return None
        weixinId = weixinIdFind.weixinId

        config_wt = online_config_weixin_template
        listFind = self.db.query(config_wt).filter(config_wt.id == weixinTemplateId).first()
        if not listFind:  # 微信模板库的id错误
            return None
        # 模板库的字段集合自动匹配数据
        fieldList = listFind.fieldList
        fieldListArr = fieldList.split(',')
        data = {}
        for fieldListArr_value in fieldListArr:
            tmp = {}
            a=1
            paramsVal = params[fieldListArr_value]
            if isinstance(paramsVal, list) or isinstance(paramsVal, dict) or isinstance(paramsVal, set):
                convertedToList = list(set(paramsVal))
                sendVal = convertedToList[0]
                sendVal = re.sub('\r\n', "&quot;", sendVal, flags=re.IGNORECASE)
                sendVal = re.sub('\\n', "“", sendVal, flags=re.IGNORECASE)
                # true_part if condition else false_part  三元运算符
                color = convertedToList[1]
                color = color if color else '#173177'  # 字体颜色
                data[fieldListArr_value] = {'value': urllib.parse.quote_plus(sendVal),
                                            'color': urllib.parse.quote_plus(color)}
            else:
                a = 1
                sendVal = re.sub('\r\n', "&quot;", paramsVal, flags=re.IGNORECASE)
                sendVal = re.sub('\\n', "“", sendVal, flags=re.IGNORECASE)
                data[fieldListArr_value] = {'value': urllib.parse.quote_plus(sendVal),
                                            'color': urllib.parse.quote_plus('#173177')}
            sendArr = {'touser': openId, 'template_id': weixinId, 'data': data}
            if 'tourl' in params:
                sendArr['url'] = urllib.parse.quote_plus(params['tourl'])

        return sendArr

    # @Explain : 发送客服消息给用户
    # @Time    : 2017/12/29
    # @Author  : gg
    def addLog(self, parameter):
        parameter['intime'] = int(time.time())
        parameter['uptime'] = int(time.time())
        parameter['indate'] = int(time.mktime(time.strptime(time.strftime('%Y-%m-%d', time.localtime()), '%Y-%m-%d')))

        try:
            rr = self.db.execute(list_log_db.__table__.insert(), parameter)
            self.db.commit()

        except:
            self.AppLogging.warning("-----写入发送客服消息log error-------")
            # print('----写入发送客服消息log error-----')

    # @Explain : 发送客服消息给用户
    # @Time    : 2017/12/29
    # @Author  : gg
    async def kf_send_message_to_weixin(self, bid, sendContent):
        #
        # weixindb = online_business_weixin_config
        # query = self.db.query(weixindb)
        # s = query.filter(weixindb.bid == bid).first()
        # weixin_AppID = s.weixin_AppID
        # weixin_AppSecret = s.weixin_AppSecret
        # print(sendContent)
        # # 导入微信类
        # from Tool.Weixin import class_weixin_api
        # wxClass = class_weixin_api(bid, weixin_AppID, weixin_AppSecret)
        # # 获取一下token
        # wxClass.getCredentialAccessTokenToWeixin()
        wxClass = self.Initialize_class_weixin(bid)
        sendReturn = await self.message_custom_send(wxClass.AccessToken, sendContent)

        if sendReturn == None:
            self.AppLogging.error('发送客服消息错误')
            raise
        jsonArr = json.loads(sendReturn)
        return jsonArr
        # return wxClass.message_custom_send(sendContent)

    # 发送客服消息
    # author     : gg
    # create date    : 2017-12-29
    async def message_custom_send(self, token, sendContent):
        url = 'https://api.weixin.qq.com/cgi-bin/message/custom/send?access_token=' + token
        tries = 0
        data = json.dumps(sendContent)
        end_data = unquote(data)
        end_data = html.unescape(end_data)
        while tries < self.max_tries:  # 取不到数据会重试N次
            try:
                response = await self.ClientSession.post(
                    url, data=end_data)
                break
            except aiohttp.ClientError:
                pass
            tries += 1
        try:
            text = await response.text()  # 异步接收返回数据

            print(len(text))
            return text
        finally:
            await response.release()  # 异步释放资源

    # @Explain : 初始化微信对象
    # @Time    : 2018/01/03
    # @Author  : gg
    def Initialize_class_weixin(self, bid):
        cacheName = 'Initialize_class_weixin_' + str(bid)
        weixindb = online_business_weixin_config
        query = self.db.query(weixindb)
        s = query.filter(weixindb.bid == bid).first()
        weixin_AppID = s.weixin_AppID
        weixin_AppSecret = s.weixin_AppSecret
        # 导入微信类
        from Tool.WeixinUpdate.Weixin import class_weixin_api
        wxClass = class_weixin_api(bid, weixin_AppID, weixin_AppSecret)
        # 获取一下token
        if isTest == 1:
            # 测试服务器
            wxClass.getCredentialAccessTokenToWeixin()
        else:
            # 正式服务器
            wxClass.getCredentialAccessToken()
        # self.session[cacheName] = wxClass
        return wxClass
        # if cacheName in self.session:
        #     return self.session[cacheName]
        # else:
        #     weixindb = online_business_weixin_config
        #     query = self.db.query(weixindb)
        #     s = query.filter(weixindb.bid == bid).first()
        #     weixin_AppID = s.weixin_AppID
        #     weixin_AppSecret = s.weixin_AppSecret
        #     # 导入微信类
        #     from Tool.Weixin import class_weixin_api
        #     wxClass = class_weixin_api(bid, weixin_AppID, weixin_AppSecret)
        #     # 获取一下token
        #     if isTest==1:
        #         #测试服务器
        #         wxClass.getCredentialAccessTokenToWeixin()
        #     else:
        #         #正式服务器
        #         wxClass.getCredentialAccessToken()
        #     self.session[cacheName] = wxClass
        #     return wxClass

    # @Explain : 执行发送
    # @Time    : 2017/12/28
    # @Author  : gg
    async def executeSend(self, parameter):
        if parameter['send_type'] == 1:
            return await self.sendWeixinKfMessage(parameter)
        else:
            return None

    # @Explain : 取得客服消息模板信息
    # @Time    : 2017/12/22
    # @Author  : gg
    def getMessageListKefuJsonString(self, doitId):
        cacheName = 'jsonStrint_' + str(doitId)
        cacheName = None
        # business_kf_message_list
        json = self.db.query(online_business_kf_message_list).filter(
            online_business_kf_message_list.id == doitId).first()
        if json == None:
            return None
        else:
            # self.session[cacheName] = json.messageJson
            return json.messageJson
        # if not cacheName is None and cacheName in self.session  :
        #     a=1
        #     return self.session[cacheName]
        # else:
        #     a=1
        #     # business_kf_message_list
        #     json = self.db.query(online_business_kf_message_list).filter(
        #         online_business_kf_message_list.id == doitId).first()
        #     if json == None:
        #         return None
        #     else:
        #         self.session[cacheName] = json.messageJson
        #         return json.messageJson

    # @Explain : 执行消息发送成功的扩展回调
    # @Time    : 2018/01/11
    # @Author  : gg
    def sendSuccessCallback(self, extensionType, parameter):
        # 执行消息发送成功的扩展回调
        extensionType=str(extensionType)
        callback = config_send_list_extension_type[extensionType]['callback']
        a=1
        if not callback is  None or callback != '':
            try:
                eval('self.' + callback + '(parameter)')
            except:
                self.AppLogging.warning('----eval失败-----')
                # print('eval失败')

    # @Explain : 消息队列、wox卡券推送成功回调
    # @Time    : 2018/01/11
    # @Author  : gg
    def woxCouponsPushCallback(self, parameter):
        return None

    # @Explain : 消息队列、微信卡券推送成功回调
    # @Time    : 2018/01/11
    # @Author  : gg
    def weixinCouponsPushCallback(self, parameter):
        return None

    # @Explain : 消息队列、卡包推送成功回调
    # @Time    : 2018/01/11
    # @Author  : gg
    # 以前写法，2018-01-19日更改新的
    # def couponsPackagePushCallback(self,parameter):
    #
    #     #生日礼包、并且是这几个商圈推送的
    #     if parameter['orderId1']==1 or parameter['orderId1']=='1':
    #         try:
    #             dblist={'birthdayGift':1}
    #             rr = self.db.query(online_userinfo_ext).filter(online_userinfo_ext.uid == parameter['userId']).update(dblist, synchronize_session=False)
    #             self.db.commit()
    #         except:
    #             self.AppLogging.warning('----db update online_userinfo_ext error-----')
    #             # print('-----db update online_userinfo_ext error-------')
    #     return 'yes'

    def couponsPackagePushCallback(self, parameter):
        # 修改生日礼包状态为发送成功
        a=1
        if parameter['orderId1'] == 1 or parameter['orderId1'] == '1':
            try:
                dblist = {'birthdayCouponsPackage': 3}
                rr = self.db.query(online_userinfo_ext).filter(online_userinfo_ext.uid == parameter['userId']).update(
                    dblist, synchronize_session=False)
                self.db.commit()
            except:
                self.AppLogging.warning('----db update online_userinfo_ext error-----')
                # print('-----db update online_userinfo_ext error-------')

            # 增加生日卡包日志、状态为已发送
            log = {
                'bid': parameter['bid'],
                'userId': parameter['userId'],
                'cardLeve': parameter['remark'],
                'status': 3,  # 参考config_birthday_couponsPackage_status
                'errMsg': '发送成功',
                'intime': int(time.time()),
                'indate': int(time.mktime(time.strptime(time.strftime('%Y-%m-%d', time.localtime()), '%Y-%m-%d'))),
            }
            try:
                rr = self.db.execute(online_birthday_coupons_package_log.__table__.insert(), log)
                self.db.commit()

            except:
                self.AppLogging.warning("-----写入online_birthday_coupons_package_log error-------")
        return 'yes'
