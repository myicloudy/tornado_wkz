# coding:utf-8
# @Explain : 说明
# @Time    : 2018/7/16 下午1:45
# @Author  : gg
# @FileName: online_analysis_user_list.py

from sqlalchemy import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine

BaseModel = declarative_base()


class online_analysis_user_list(BaseModel):
    __tablename__ = 'online_analysis_user_list'
    metadata = MetaData(engine)

    # 把表映射过来
    Table(__tablename__, metadata, autoload=True)

