#! python2
# coding: utf-8

# final version

import codecs
import MySQLdb

# ------------------- SQL BEGIN ---------------------------

# SQL 插入t_user语句
sql_user = "INSERT IGNORE INTO t_user (user_id, profilename) VALUES (%s, %s)"

# SQL 插入t_review语句
sql_review = """INSERT INTO t_review
(user_id, movie_id, helpfulness, score, summary, text, timestamp)
VALUES (%s, %s, %s, %s, %s, %s, %s)
"""

# --------------------- SQL END ------------------------


# -----------------  VARIABLE BEGIN --------------------

# 储存有sql语句的param元组的list
param_user = []
param_review = []

productId = ''
userId = ''
profileName = ''
helpfulness = ''
score = ''
timestamp = ''
summary = ''
text = ''

# ------------------ VARIABLE END ----------------------

# 打开数据库连接
db = MySQLdb.connect("localhost", "root", "123456", "mixeddb")

db.set_character_set('utf8')

# 使用cursor()方法获取操作游标
cursor = db.cursor()

with codecs.open('../data/movies.txt', encoding='iso-8859-1') as f:
    nrow = 0
    for line in f:
        nrow += 1
        colonPos = line.find(':')
        prefix = line[:colonPos]

        if prefix == 'product/productId':
            productId = line[colonPos + 2:len(line) - 1]
        elif prefix == 'review/userId':
            userId = line[colonPos + 2:len(line) - 1]
        elif prefix == 'review/profileName':
            profileName = line[colonPos + 2:len(line) - 1]
        elif prefix == 'review/helpfulness':
            helpfulness = line[colonPos + 2:len(line) - 1]
        elif prefix == 'review/score':
            score = line[colonPos + 2:len(line) - 1]
        elif prefix == 'review/time':
            timestamp = line[colonPos + 2:len(line) - 1]
        elif prefix == 'review/summary':
            summary = line[colonPos + 2:len(line) - 1]
        elif prefix == 'review/text':
            text = line[colonPos + 2:len(line) - 1]

        if (nrow - 8) % 9 == 0:
            param_user.append((userId, profileName))
            param_review.append((userId, productId, helpfulness, score, summary, text, timestamp))

        if nrow % 10000 == 0:
            # execute_sql(cursor, sql_user, param_user, 't_user')
            # execute_sql(cursor, sql_review, param_review, 't_review')
            cursor.executemany(sql_user, param_user)
            cursor.executemany(sql_review, param_review)
            db.commit()
            param_user = []
            param_review = []
            print nrow, ' done!'


cursor.executemany(sql_user, param_user)
cursor.executemany(sql_review, param_review)
db.commit()

f.close()