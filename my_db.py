#!/usr/bin/env python
# encoding: utf-8
#
# Copyright (c) 2018 alibaba-inc. All Rights Reserved
#

"""
File: orm.py
Date: 2019-01-06 19:10
Author: wang.gaofei@alibaba-inc.com
"""

# 导入:
from sqlalchemy import Column, create_engine, String, Integer, Date, DECIMAL, Boolean
from sqlalchemy.orm import sessionmaker
from sqlalchemy.ext.declarative import declarative_base


# 创建对象的基类:
Base = declarative_base()

# 定义User对象:
class MoviesTable(Base):
    # 表的名字:
    __tablename__ = 'movies'

    # 表的结构:
    id = Column(Integer, primary_key=True,autoincrement=True)
    name = Column(String(1024),nullable=True)
    score = Column(DECIMAL(2,1),default=0)
    hot = Column(Integer,default=0)
    show_time = Column(Date,nullable=True)
    status = Column(Boolean,default=True)
    down_time = Column(Date)
    link = Column(String(1024), nullable=True)

# 初始化数据库连接:
engine = create_engine('mysql+mysqlconnector://root:root@localhost:3306/cinema')

Base.metadata.create_all(engine) #创建表结构

# 创建DBSession类型:
DBSession = sessionmaker(bind=engine)


def connect_db():
    # 创建session对象:
    session = DBSession()
    return session


def test_write_data(session):
    # 创建新User对象:
    new_movie = MoviesTable(id='2', name=u'你好', link="www.baidu.com", show_time='2019-01-01')
    # 添加到session:
    session.add(new_movie)
    # 提交即保存到数据库:
    session.commit()
    return


def close_db(session):
    # 关闭session:
    session.close()
    return

if __name__ == '__main__':
    s = connect_db()
    test_write_data(s)
    close_db(s)
