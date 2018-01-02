#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb

# ---------------------SQL START----------------------



# ----------------SQL END------------------


# 打开数据库连接
db = MySQLdb.connect("localhost","root","123456","datawarehouse" )

# 使用cursor()方法获取操作游标
cursor = db.cursor()


db.commit()

# 关闭数据库连接
db.close()
