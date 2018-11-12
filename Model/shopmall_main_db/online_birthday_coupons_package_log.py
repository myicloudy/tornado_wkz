# coding:utf-8
# @Explain : 说明
# @Time    : 2018/1/19 上午9:03
# @Author  : gg
# @FileName: online_birthday_coupons_package_log.py

from sqlalchemy import Table, MetaData
from sqlalchemy.ext.declarative import declarative_base

from Config.sqlalchemy import engine

BaseModel = declarative_base()


class online_birthday_coupons_package_log(BaseModel):
    __tablename__ = 'online_birthday_coupons_package_log'
    metadata = MetaData(engine)

    # 把表映射过来
    Table(__tablename__, metadata, autoload=True)

