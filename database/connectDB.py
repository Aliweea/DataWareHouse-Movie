#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb

# 打开数据库连接
db = MySQLdb.connect("localhost","root","123456","test" )

# 使用cursor()方法获取操作游标
cursor = db.cursor()

# SQL 插入语句
sql = "INSERT INTO EMPLOYEE" \
      "(FIRST_NAME, LAST_NAME, AGE, SEX, INCOME) " \
      "VALUES ('%s', '%s', '%d', '%c', '%d')"

# SQL 插入语句
sql2 = "INSERT INTO TEST (NAME) VALUES ('%s')"

try:
   # 执行sql语句
   cursor.execute(sql2 % \
                  ('Mac'))
   # 提交到数据库执行
   db.commit()
except:
   # Rollback in case there is any error
   db.rollback()


# 关闭数据库连接
db.close()
