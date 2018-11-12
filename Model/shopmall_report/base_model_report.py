# coding:utf-8
# @Explain : 说明
# @Time    : 2018/8/16 下午12:00
# @Author  : yt
# @FileName: base_model_report.py

from sqlalchemy import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine_report

BaseModel = declarative_base()


def base_model_report(tableName):
    class BaseClass(BaseModel):
        __tablename__ = tableName
        metadata = MetaData(engine_report)

        # 把表映射过来
        Table(__tablename__, metadata, autoload=True)
    return BaseClass
