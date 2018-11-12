# coding:utf-8
# @Explain : 说明
# @Time    : 2017/11/27 下午8:58
# @Author  : gg
# @FileName: online_business_kf_message_list.py

from sqlalchemy import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine

BaseModel = declarative_base()


class online_business_kf_message_list(BaseModel):
    __tablename__ = 'online_business_kf_message_list'
    metadata = MetaData(engine)

    # 把表映射过来
    Table(__tablename__, metadata, autoload=True)

