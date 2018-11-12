# coding:utf-8
import os.path
import tornado.httpserver
import tornado.ioloop
import tornado.options
import tornado.web
from tornado.options import define, options
from Urls import urls
import cfg
from Tool import UUID
from Config.sqlalchemy import engine,engine_conduct,engine_analysis,engine_report
from Config.sqlalchemy import engine_db_2018,engine_pay_2018,engine_user_2018
from pymongo import MongoClient
import redis
# from Config.sqlalchemy import engine
define("port", default=9004, help="run on the given port", type=int)
import datetime

from sqlalchemy.orm import scoped_session, sessionmaker
def f2s():
    print ('2s ', datetime.datetime.now())

class Application(tornado.web.Application):
    def __init__(self):
        handlers = urls
        settings = dict(
            template_path=os.path.join(os.path.dirname(__file__), "Templates"),
            static_path=os.path.join(os.path.dirname(__file__), "Static"),
            cookie_secret=UUID.get_uuid(),
            xsrf_cookies=True,
            login_url="/login",
            debug=options.debug,
            autoescape=None,
            compress_whitespace=False,
            # session config by memory
            driver="memory",
            driver_settings=dict(
                host={},
            ),
            # session config by memcached
            # driver="memcached",
            # driver_settings=dict(
            #     host='120.25.122.48',
            #     port=11211
            # ),
            # sid_name='torndsession-mem',  # default is msid.
            session_lifetime=1200,  # default is 1200 seconds.
            force_persistence=True,
            # session config by redis
            # driver="redis",
            # driver_settings=dict(
            #     host='localhost',
            #     port=6379,
            #     db=0,
            #     max_connections=1024,
            # )
        )
        tornado.web.Application.__init__(self, handlers, **settings)

        self.db = scoped_session(
            sessionmaker(
                bind=engine,
                autocommit=False,
                autoflush=True,
                expire_on_commit=False
            )
        )
        self.db_conduct = scoped_session(
            sessionmaker(
                bind=engine_conduct,
                autocommit=False,
                autoflush=True,
                expire_on_commit=False
            )
        )
        self.db_analysis = scoped_session(
            sessionmaker(
                bind=engine_analysis,
                autocommit=False,
                autoflush=True,
                expire_on_commit=False
            )
        )
        self.db_report = scoped_session(
            sessionmaker(
                bind=engine_report,
                autocommit=False,
                autoflush=True,
                expire_on_commit=False
            )
        )
        self.db_mongo = MongoClient(options.mongo_content_str).admin
        # self.con_redis=redis.Redis(connection_pool=redis.ConnectionPool(host=options.redis_host, port=int(options.redis_port)))
        self.con_redis = redis.Redis(host=options.redis_host, port=int(options.redis_port), decode_responses=True)
        self.db_2018 = scoped_session(
            sessionmaker(
                bind=engine_db_2018,
                autocommit=False,
                autoflush=True,
                expire_on_commit=False
            )
        )
        self.pay_2018 = scoped_session(
            sessionmaker(
                bind=engine_pay_2018,
                autocommit=False,
                autoflush=True,
                expire_on_commit=False
            )
        )
        self.user_2018 = scoped_session(
            sessionmaker(
                bind=engine_user_2018,
                autocommit=False,
                autoflush=True,
                expire_on_commit=False
            )
        )

if __name__ == "__main__":
    cfg.set_define()
    # 单线程调试
    tornado.options.parse_command_line()
    http_server = tornado.httpserver.HTTPServer(Application())
    http_server.listen(options.port)
    # tornado.ioloop.PeriodicCallback(f2s, 2000).start()  # start scheduler 每隔2s执行一次f2s
    tornado.ioloop.IOLoop.current().start()
    # 多线程部署
    # n=0
    # tornado.options.parse_command_line()
    # sockets = tornado.netutil.bind_sockets(options.port)
    # tornado.process.fork_processes(n)
    # http_server = tornado.httpserver.HTTPServer(Application())
    # http_server.add_sockets(sockets)
    # tornado.ioloop.IOLoop.instance().start()
