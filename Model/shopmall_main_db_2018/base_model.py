# coding:utf-8
# @Explain : 说明
# @Time    : 2018/7/16 下午1:59
# @Author  : gg
# @FileName: base_mode.py

from sqlalchemy import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine_db_2018

BaseModel = declarative_base()


def base_mode_2018(tableName):
    class BaseClass(BaseModel):
        __tablename__ = tableName
        metadata = MetaData(engine_db_2018)

        # 把表映射过来
        Table(__tablename__, metadata, autoload=True)
    return BaseClass
