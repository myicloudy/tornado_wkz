# coding:utf-8
# @Explain : 说明
# @Time    : 2017/11/27 下午8:58
# @Author  : gg
# @FileName: online_userinfo_weixin.py

from sqlalchemy import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine

BaseModel = declarative_base()


class online_behavior_analysis_list(BaseModel):
    __tablename__ = 'online_behavior_analysis_list'
    metadata = MetaData(engine)

    # 把表映射过来
    Table(__tablename__, metadata, autoload=True)

