# coding:utf-8
# @Explain : 数据库连接池配置
# @Time    : 2017/11/27 下午4:53
# @Author  : gg
# @FileName: sqlalchemy.py
import configparser
conf = configparser.ConfigParser()
conf.read("Config/mall.conf")
from sqlalchemy import create_engine
import cfg



engine = create_engine(
    'mysql+pymysql://%s:%s@%s:%d/%s' % (
        cfg.get_db_user(),
        cfg.get_db_pass(),
        cfg.get_db_host(),
        cfg.get_db_port(),
        cfg.get_db_database()
    ),
    encoding='utf-8',
    echo=True,
    pool_size=1000,
    pool_recycle=7200,
    connect_args={'charset': 'utf8mb4'}
)
#行为库
engine_conduct = create_engine(
    'mysql+pymysql://%s:%s@%s:%d/%s' % (
        conf.get("db2", "user"),
        conf.get("db2", "pass"),
        conf.get("db2", "host"),
        conf.getint("db2", "port"),
        conf.get("db2", "database")
    ),
    encoding='utf-8',
    echo=True,
    pool_size=1000,
    pool_recycle=7200,
    connect_args={'charset': 'utf8mb4'}
)
#分析器库shopmall_analysis
engine_analysis = create_engine(
    'mysql+pymysql://%s:%s@%s:%d/%s' % (
        conf.get("db3", "user"),
        conf.get("db3", "pass"),
        conf.get("db3", "host"),
        conf.getint("db3", "port"),
        conf.get("db3", "database")
    ),
    encoding='utf-8',
    echo=True,
    pool_size=1000,
    pool_recycle=7200,
    connect_args={'charset': 'utf8mb4'}
)

#统计数据库shopmall_report
engine_report = create_engine(
    'mysql+pymysql://%s:%s@%s:%d/%s' % (
        conf.get("db4", "user"),
        conf.get("db4", "pass"),
        conf.get("db4", "host"),
        conf.getint("db4", "port"),
        conf.get("db4", "database")
    ),
    encoding='utf-8',
    echo=True,
    pool_size=1000,
    pool_recycle=7200,
    connect_args={'charset': 'utf8mb4'}
)
#新版数据库
engine_db_2018 = create_engine(
    'mysql+pymysql://%s:%s@%s:%d/%s' % (
        conf.get("db_new_2018", "user"),
        conf.get("db_new_2018", "pass"),
        conf.get("db_new_2018", "host"),
        conf.getint("db_new_2018", "port"),
        conf.get("db_new_2018", "database")
    ),
    encoding='utf-8',
    echo=True,
    pool_size=1000,
    pool_recycle=7200,
    connect_args={'charset': 'utf8mb4'}
)
#新版支付数据库
engine_pay_2018 = create_engine(
    'mysql+pymysql://%s:%s@%s:%d/%s' % (
        conf.get("db_pay_2018", "user"),
        conf.get("db_pay_2018", "pass"),
        conf.get("db_pay_2018", "host"),
        conf.getint("db_pay_2018", "port"),
        conf.get("db_pay_2018", "database")
    ),
    encoding='utf-8',
    echo=True,
    pool_size=1000,
    pool_recycle=7200,
    connect_args={'charset': 'utf8mb4'}
)

#新版用户数据库
engine_user_2018 = create_engine(
    'mysql+pymysql://%s:%s@%s:%d/%s' % (
        conf.get("db_user_2018", "user"),
        conf.get("db_user_2018", "pass"),
        conf.get("db_user_2018", "host"),
        conf.getint("db_user_2018", "port"),
        conf.get("db_user_2018", "database")
    ),
    encoding='utf-8',
    echo=True,
    pool_size=1000,
    pool_recycle=7200,
    connect_args={'charset': 'utf8mb4'}
)