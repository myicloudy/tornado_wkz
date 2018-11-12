# coding:utf-8
# @Explain : 说明
# @Time    : 2018/1/15 上午11:58
# @Author  : gg
# @FileName: online_pdf.py

from sqlalchemy import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine

BaseModel = declarative_base()


class online_pdf(BaseModel):
    __tablename__ = 'online_pdf'
    metadata = MetaData(engine)

    # 把表映射过来
    Table(__tablename__, metadata, autoload=True)
