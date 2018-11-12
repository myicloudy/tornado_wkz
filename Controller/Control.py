from torndsession.sessionhandler import SessionBaseHandler
from Tool.Mrcache.Memcached import pyMemcached
import time
from DefaultValue import DefaultValues
import random
import json
from tornado.web import Finish
from Tool.Idcrypt import Idcrypt
import ast
# @Explain : 项目公共方法
# @Time    : 2017/11/24 下午2:40
# @Author  : gg
class ControlHandler(SessionBaseHandler):
    def initialize(self):
        '''
        类初始化 1.initialize 2.perpare
        :return: 
        '''
        self.DefaultValues = DefaultValues
    def decrypyBid(self,bidStr,**parms):
        '''
        解密bid
        :param bidStr: 
        :return: 
        '''
        scr = Idcrypt()
        bid=scr.decrypt(bidStr)
        isReturn=parms.get('isReturn', False)
        if not str(bid).isdigit() or not bid:
            if isReturn:
                return None
            else:
                self.api_return(80004, '商圈id错误')
        return bid

    def encrypyBid(self,bid,**parms):
        '''
        解密bid
        :param bidStr: 
        :return: 
        '''
        scr = Idcrypt()
        bidStr=scr.encrypt(bid)
        isReturn=parms.get('isReturn', False)
        if not bidStr:
            if isReturn:
                return None
            else:
                self.api_return(80005, '商圈加密错误')
        return bid

    def is_json(self,value):
        '''
        判断是否是json
        :param value: 
        :return: 
        '''
        try:
            json_object = json.loads(value.decode('utf-8').replace("'", '"'),encoding='utf-8')
        except:
            self.api_return(80003, '数据不是json格式')
        return json_object
    def api_return(self,errCode=0,errMsg='',**parms):
        '''
        接口返回
        :param errCode: 
        :param errMsg: 
        :param isReturn: 为真是返回
        :return: 
        '''
        d={'errCode':errCode,'errMsg':errMsg}
        d={**d,**parms}
        if parms.get('isReturn',False):
            return d
        else:
            self.set_header("Content-Type", "application/json; charset=UTF-8")
            self.write(json.dumps(d,ensure_ascii=False))
            raise Finish()

    def to_not_empty(self,key,value,**parms):
        '''
        接口参数不能为空判断
        :param key: key
        :param value: 传入值
        :param title: 标题
        :param str_len: 长度
        :param errCode: 错误编码
        :param dict: 上一次的dict
        :param is_int: 
        :return: 
        '''
        title=parms.get('title','')#出错标题
        str_len=parms.get('str_len',20)#值长度
        dict=parms.get('dict',{})#上一个dict
        is_int=parms.get('is_int',False)#是否是int类型
        value=self.sql_filter(str(value))
        if not value:
            self.api_return(80001, title + '参数为空')
        if is_int:
            if not str(value).isdigit():
                self.api_return(80000, title + '参数不是数字类型')
        if len(str(value))>str_len:
            self.api_return(80002, title + '参数超出定义长度')
        tmp={key:value}
        return tmp



    def sql_filter(self,sql):
        '''
        sql防注入
        :return: 
        '''
        dirty_stuff = ["\"", "\\", "/", "*", "'", "=", "-", "#", ";", "<", ">", "+", "%", "$", "(", ")", "%", "@", "!"]
        for stuff in dirty_stuff:
            sql = sql.replace(stuff, "")
        return sql



    def get_order_num(self):
        '''
        生成订单号
        :return: 
        '''
        return str(int(time.time())) + str(random.randint(1111111111, 9999999999))
    def timestampToWeek(self,timestamp):
        '''
        时间戳返回星期几
        :param timestamp: 
        :return: 
        '''
        week_day_dict = {
            1: '星期一',
            2: '星期二',
            3: '星期三',
            4: '星期四',
            5: '星期五',
            6: '星期六',
            0: '星期日',
        }
        dweek = time.strftime("%w", time.localtime(timestamp))
        return week_day_dict[int(dweek)]

    def dbtime(self,tmp:dict={},type='insert'):
        '''
        数据库插入自动三个对象,
        :return: 
        '''
        if type=='insert':
            tmp['intime'] = int(time.time())
            tmp['uptime'] = int(time.time())
            tmp['indate'] = int(time.mktime(time.strptime(time.strftime('%Y-%m-%d', time.localtime()), '%Y-%m-%d')))
        elif type=='update':
            tmp['uptime'] = int(time.time())
        return tmp
    # class ControlHandler(tornado.web.RequestHandler):
    @property  # 可以作为属性的装饰器 行为库
    def db_analysis(self):
        return self.application.db_analysis
    @property  # 可以作为属性的装饰器 行为库
    def db_conduct(self):
        return self.application.db_conduct
    @property  # 可以作为属性的装饰器
    def db(self):
        return self.application.db

    @property  # 可以作为属性的装饰器
    def db_report(self):
        return self.application.db_report

    @property  # 可以作为属性的装饰器 默认选择主库，如果有多个选择库，在建方法
    def db_mongo(self):
        return self.application.db_mongo.client['wox_mongodb_2018']

    @property  # 可以作为属性的装饰器 默认选择主库，如果有多个选择库，在建方法
    def redis(self):
        return self.application.con_redis

    @property  # 可以作为属性的装饰器 行为库
    def db_2018(self):#新版数据库
        return self.application.db_2018

    @property  # 可以作为属性的装饰器 行为库
    def pay_2018(self):  # 新版支付数据库
        return self.application.pay_2018

    @property  # 可以作为属性的装饰器 行为库
    def user_2018(self):  # 新版用户数据库
        return self.application.user_2018

    # @property
    def get_dictionary(self, title=''):
        # value = self.redis.get(title)   # b"{'10': {'valName': '积分+微信', 'callBack': ''}}"
        # value = bytes.decode(value)  # "{'10': {'valName': '积分+微信', 'callBack': ''}}"
        # val_dict = ast.literal_eval(value)  # {'10': {'valName': '积分+微信', 'callBack': ''}}
        return ast.literal_eval(self.redis.get[title])
    # @Explain : 实例化结束后关闭数据库连接
    # @Time    : 2017/11/27 16：06
    # @Author  : gg
    def on_finish(self):
        self.db.close()
        self.db_conduct.close()
        self.db_analysis.close()
    @property
    def get_remote_ip(self):
        '''
        获取当前ip
        :return: ip
        '''
        x_real_ip = self.request.headers.get("X-Real-IP",'')
        remote_ip = x_real_ip or self.request.remote_ip
        return remote_ip

    # @Explain : 抛出异常函数
    # @Time    : 2018/06/19
    # @Author  : gg
    def writeWebError(self,code,message):
        self.finish("<html><title>%(code)d: %(message)s</title>"
                    "<body>%(code)d: %(message)s</body></html>" % {
                        "code": code,
                        "message": message,
                    })

    # @Explain : 公共日志方法
    # @Time    : 2017/11/27 下午9:26
    # @Author  : gg
    # @url     : http://yshblog.com/blog/125
    @property
    def AppLogging(self):
        import logging.config
        from Config.logging import LOGGING
        # 加载前面的标准配置
        logging.config.dictConfig(LOGGING)
        # 获取loggers其中的一个日志管理器
        logger = logging.getLogger("default")
        return logger



    # @property  # 可以作为属性的装饰器
    # def db2(self):
    #     return self.application.db2

    # 用缓存读取
    # author     : gg
    # create date    : 2018-06-19
    def mrcacheGet(self, cname):
        mcache = self.pyMamcached()
        return mcache.get(cname)

    # 用缓存存储
    # author     : gg
    # create date    : 2018-06-19

    def mrcacheSet(self, cname, value, express=0):
        mcache = self.pyMamcached()
        return mcache.set(cname, value, express)

    # 用缓存删除
    # author     : gg
    # create date    : 2018-06-19

    def mrcacheDelete(self, cname):
        mcache = self.pyMamcached()
        return mcache.delete(cname)

    # 用memcached
    # author     : gg
    # create date    : 2018-06-19
    def pyMamcached(self):
        mc = pyMemcached.create(self)
        return mc

class ErrorHandler(SessionBaseHandler):
    def get(self, *args, **kwargs):
        self.write('<h1 color="red">404,no page!</h1>')