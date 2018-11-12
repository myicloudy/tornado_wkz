# coding:utf-8
# @Explain : 说明
# @Time    : 2017/11/27 下午8:50
# @Author  : gg
# @FileName: online_userinfo_ext.py

from sqlalchemy import Table,MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine

BaseModel = declarative_base()


class online_userinfo_ext(BaseModel):
    __tablename__ = 'online_userinfo_ext'
    metadata = MetaData(engine)

    #把表映射过来
    Table(__tablename__, metadata, autoload=True)
