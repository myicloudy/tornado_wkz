# coding:utf-8
# @Explain : 说明
# @Time    : 2018/1/15 下午12:00
# @Author  : gg
# @FileName: online_pdf_list.py

from sqlalchemy import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine

BaseModel = declarative_base()


class online_pdf_list(BaseModel):
    __tablename__ = 'online_pdf_list'
    metadata = MetaData(engine)

    # 把表映射过来
    Table(__tablename__, metadata, autoload=True)
