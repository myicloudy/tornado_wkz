# coding:utf-8

import json
import logging.config
import urllib.parse
from Tool.Crontab.CheckParams import CheckParams
from Tool.Crontab.CrontabFuc import Mycron
# 加载前面的标准配置
from Config.logging import LOGGING

logging.config.dictConfig(LOGGING)


# @Explain : 入口方法
# @Time    : 2018/08/20
# @Author  : mzf


err_msg = {
    0: '任务成功',
    1: '没有任务',
    30000: '写入任务失败',
    30001: '昵称已存在',
    30002: '昵称不存在',
    30003: "删除任务成功",
    30004: '删除任务失败',
    30005: ' 不允许参数有空值',

    30006: '传入值太多，每个参数不能超过10个',
    30007: '传入值有误，不在正常范围',
    30008: '含有不符合规范的字符',
    30009: '格式输入不合法',
}


def check_int(res):
    """
    查看是否为int类型
    :param res: 参数
    :return: 参数 or 1
    """
    if isinstance(res, int):
        ret = {'errCode': res, 'errMsg': err_msg.get(res)}
        return ret
    return 1


class InsertHandler(CheckParams):
    def prepare(self):
        self.my_cron = Mycron()

    def get(self, *args, **kwargs):
        """
        处理添加定时任务
        minute(0-59)
        hour(0-23)
        day of month(1-31)
        month(1-12)
        day of week(0-6)(sunday to saturday;7 is also Sunday on some system)
        """
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        minute = self.get_query_argument('c1', '')
        hour = self.get_query_argument('c2', '')
        day_of_mouth = self.get_query_argument('c3', '')
        month = self.get_query_argument('c4', '')
        day_of_week = self.get_query_argument('c5', '')
        shell = self.get_query_argument('shell', '')
        nickname = self.get_query_argument('nickname', '')
        shlist = [urllib.parse.unquote(ch) for ch in [minute, hour, day_of_mouth, month, day_of_week, shell, nickname]]
        keys = ['c1', 'c2', 'c3', 'c4', 'c5', 'shell', 'nickname']
        param_dict = dict(zip(keys, shlist))
        val = self.null_params(param_dict)
        if val == '':
            ret = {'errCode': val, 'errMsg': (val + err_msg.get(30005))}
            self.write(json.dumps(ret, ensure_ascii=False))
        else:
            yzminute = self.yz_minute(param_dict['c1'], 0, 60)
            yzhour = self.yz_hours_to_week(param_dict['c2'], 0, 24)
            yzday = self.yz_hours_to_week(param_dict['c3'], 1, 32)
            yzmouth = self.yz_hours_to_week(param_dict['c4'], 1, 13)
            yzweek = self.yz_hours_to_week(param_dict['c5'], 0, 7)
            hlist = [yzminute, yzhour, yzday, yzmouth, yzweek]
            for ch in hlist:
                if check_int(ch) != 1:
                    self.write(json.dumps(check_int(ch), ensure_ascii=False))
                    break
            else:
                find_comment = self.my_cron.find_for_nickname(nickname)
                if find_comment == 30002:
                    result = self.my_cron.insertcron(' '.join(hlist), shell, nickname)
                    ret = {'errCode': result, 'errMsg': err_msg.get(result)}
                    self.write(json.dumps(ret, ensure_ascii=False))
                else:
                    ret = {'errCode': find_comment, 'errMsg': err_msg.get(find_comment)}
                    self.write(json.dumps(ret, ensure_ascii=False))


class QueryHandler(CheckParams):
    def prepare(self):
        self.my_cron = Mycron('root')

    def get(self):
        """
        查看定时任务
        :return:
        """
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        data = self.get_query_argument('data', '')
        comment = self.get_query_argument('nickname', '')
        if data == 'all':
            result = self.my_cron.look_crontab()
            if result == 1:
                ret = {'errCode': result, 'errMsg': err_msg.get(result)}
                self.write(json.dumps(ret, ensure_ascii=False))
            else:
                ret = {'errCode': 0, 'errMsg': err_msg.get(0), 'data': [str(i) for i in result]}
                self.write(json.dumps(ret, ensure_ascii=False))
        elif data == 'one':
            one = self.my_cron.find_one_nickname(comment)
            print(one)
            if one == 30002:
                ret = {'errCode': one, 'errMsg': err_msg.get(one)}
                self.write(json.dumps(ret, ensure_ascii=False))
            else:
                ret = {'errCode': 0, 'errMsg': err_msg.get(0), 'data': list(str(one))}
                self.write(json.dumps(ret, ensure_ascii=False))


class UpdateHandler(CheckParams):
    def prepare(self):
        self.my_cron = Mycron('root')

    def get(self, *args, **kwargs):
        """
        更新定时任务
        :param args:
        :param kwargs:
        :return:
        """
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        minute = self.get_query_argument('c1', '')
        hour = self.get_query_argument('c2', '')
        day_of_mouth = self.get_query_argument('c3', '')
        month = self.get_query_argument('c4', '')
        day_of_week = self.get_query_argument('c5', '')
        nickname = self.get_query_argument('nickname', '')
        shlist = [urllib.parse.unquote(ch) for ch in [minute, hour, day_of_mouth, month, day_of_week, nickname]]
        keys = ['c1', 'c2', 'c3', 'c4', 'c5', 'shell', 'nickname']
        param_dict = dict(zip(keys, shlist))
        val = self.null_params(param_dict)
        if val == '':
            ret = {'errCode': val, 'errMsg': (val + err_msg.get(30005))}
            self.write(json.dumps(ret, ensure_ascii=False))
        else:
            yzminute = self.yz_minute(param_dict['c1'], 0, 60)
            yzhour = self.yz_hours_to_week(param_dict['c2'], 0, 24)
            yzday = self.yz_hours_to_week(param_dict['c3'], 1, 32)
            yzmouth = self.yz_hours_to_week(param_dict['c4'], 1, 13)
            yzweek = self.yz_hours_to_week(param_dict['c5'], 0, 7)
            hlist = [yzminute, yzhour, yzday, yzmouth, yzweek]
            for ch in hlist:
                if check_int(ch) != 1:
                    self.write(json.dumps(check_int(ch), ensure_ascii=False))
                    break
            else:
                find_comment = self.my_cron.find_for_nickname(nickname)
                if find_comment == 30001:
                    result = self.my_cron.updcrontab(' '.join(hlist), nickname)
                    ret = {'errCode': result, 'errMsg': err_msg.get(result)}
                    self.write(json.dumps(ret, ensure_ascii=False))
                else:
                    ret = {'errCode': find_comment, 'errMsg': err_msg.get(find_comment)}
                    self.write(json.dumps(ret, ensure_ascii=False))


class DelHandler(CheckParams):
    def prepare(self):
        self.my_cron = Mycron('root')

    def get(self, *args, **kwargs):
        """
        删除定时任务
        :param args:
        :param kwargs:
        :return:
        """
        self.set_header('Content-Type', 'application/json; charset=UTF-8')
        nickname = self.get_query_argument('nickname', '')
        find_comment = self.my_cron.find_for_nickname(nickname)
        if find_comment == 30001:
            result = self.my_cron.del_one_crontab(nickname)
            ret = {'errCode': result, 'errMsg': err_msg.get(result)}
            self.write(json.dumps(ret, ensure_ascii=False))
        else:
            ret = {'errCode': find_comment, 'errMsg': err_msg.get(find_comment)}
            self.write(json.dumps(ret, ensure_ascii=False))


