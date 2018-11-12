# coding:utf-8
# @Explain : 说明
# @Time    : 2018/7/16 下午1:59
# @Author  : gg
# @FileName: base_mode.py

from sqlalchemy import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine_conduct

BaseModel = declarative_base()


def base_mode_conduct(tableName):
    class BaseClass(BaseModel):
        __tablename__ = tableName
        metadata = MetaData(engine_conduct)

        # 把表映射过来
        Table(__tablename__, metadata, autoload=True)
    return BaseClass
