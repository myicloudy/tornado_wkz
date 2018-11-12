# coding:utf-8
# @Explain : 说明
# @Time    : 2018/8/16 下午4:29
# @Author  : gg
# @FileName: shopmall_main_db.py
from sqlalchemy.orm import scoped_session, sessionmaker
from sqlalchemy import create_engine
import configparser
conf = configparser.ConfigParser()
conf.read("../../Config/mall.conf")

engine = create_engine(
    'mysql+pymysql://%s:%s@%s:%d/%s' % (
        conf.get("db", "user"),
        conf.get("db", "pass"),
        conf.get("db", "host"),
        conf.getint("db", "port"),
        conf.get("db", "database")
    ),
    encoding='utf-8',
    echo=True,
    pool_size=1000,
    pool_recycle=7200,
    connect_args={'charset': 'utf8mb4'}
)


self_db = scoped_session(
    sessionmaker(
        bind=engine,
        autocommit=False,
        autoflush=True,
        expire_on_commit=False
    )
)
