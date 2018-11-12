# coding:utf-8
# @Explain : 说明
# @Time    : 2017/11/27 下午8:45
# @Author  : gg
# @FileName: online_provinces.py

from sqlalchemy import Table,MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine

BaseModel = declarative_base()


class online_provinces(BaseModel):
    __tablename__ = 'online_provinces'
    metadata = MetaData(engine)

    #把表映射过来
    Table(__tablename__, metadata, autoload=True)
