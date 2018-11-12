# coding:utf-8
# @Explain : 说明
# @Time    : 2018/3/12 下午8:02
# @Author  : gg
# @FileName: online_userinfo.py

from sqlalchemy import Table,MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine

BaseModel = declarative_base()


class online_userinfo(BaseModel):
    __tablename__ = 'online_userinfo'
    metadata = MetaData(engine)

    #把表映射过来
    Table(__tablename__, metadata, autoload=True)