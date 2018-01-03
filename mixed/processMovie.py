#!/usr/bin/python
# -*- coding: UTF-8 -*-

import codecs
import json

from relation.preprocess import *
from mixed.createTable import *

# ------------------- SQL BEGIN ---------------------------

# SQL 插入t_director语句
sql_director = "INSERT IGNORE INTO t_director (director_name) VALUES (%s)"

# SQL 插入t_actor语句
sql_actor = "INSERT IGNORE INTO t_actor (actor_name) VALUES (%s)"

# SQL 插入t_time语句
sql_time = "INSERT IGNORE INTO t_time (year, month, day, season, weekday) VALUES (%s, %s, %s, %s, %s)"

# SQL 插入t_movie语句
sql_movie = """INSERT IGNORE INTO t_movie
(product_id, movie_name, average_rating, studio, mpaa_rating, runtime, description, rank, year, month, day,
language_name, genre_name, director_id, actor_id, review_id)
SELECT * FROM
(SELECT %s as col_1, %s as col_2, %s as col_3, %s as col_4, %s as col_5, %s as col_6, %s as col_7,
%s as col_8, %s as col_9, %s as col_10, %s as col_11, %s as col_12, %s as col_13,
td.director_id as col_14, ta.actor_id as col_15, %s as col_16
FROM t_director td, t_actor ta
WHERE td.director_name=%s and ta.actor_name=%s) AS tb;
"""

# # SQL 插入t_movie语句
# sql_movie = """INSERT IGNORE INTO t_movie
# (movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description, rank, year, month, day,
# genre_name, language_name, director_id, actor_id, review_id)
# VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
# """
#
# # SQL 通过director_name查找director_id
# query_director = "SELECT director_id FROM t_director WHERE director_name=%s"
#
# # SQL 通过actor_name查找actor_id
# query_actor = "SELECT actor_id FROM t_actor WHERE actor_name=%s"

# SQL 通过movie_id查找review_id
query_review = """SELECT review_id FROM t_review WHERE movie_id=%s"""

# SQL 删除t_review的movie_id字段
delete_review_col = "ALTER TABLE `mixeddb`.`t_review` DROP COLUMN `movie_id`;"

# --------------------- SQL END ------------------------


# -----------------  VARIABLE BEGIN --------------------

# 储存有sql语句的param元组的list
param_director = []
param_actor = []
param_time = []
param_movie = []

movie_id = ''
movie_name = ''
languages = ''
directors = ''
starring = ''
supporting_actors = ''
release_date = ''
average_rating = ''
studio = ''
mpaa_rating = ''
genres = ''
runtime = ''
description = ''
rank = ''

# ------------------ VARIABLE END ----------------------


def process_line_data():
    global release_date, param_time
    if release_date is not None:
        date_time = time.strptime(release_date, '%B %d, %Y')
        season = getSeason(date_time.tm_mon)
        param_time.append((date_time.tm_year, date_time.tm_mon, date_time.tm_mday, season, date_time.tm_wday))
        process_language(date_time)
    else:
        process_language(None)


def process_language(date_time):
    global languages
    if languages is not None:
        languages = languages.split(", ")
        for language_name in languages:
            process_genre(date_time, language_name)
    else:
        process_genre(date_time, None)


def process_genre(date_time, language_name):
    global genres
    if genres is not None:
        for genre_name in genres:
            process_director(date_time, language_name, genre_name)
    else:
        process_director(date_time, language_name, None)


def process_director(date_time, language_name, genre_name):
    global directors, param_director
    if directors is not None:
        for director_name in directors:
            param_director.append((director_name,))
            process_actor(date_time, language_name, genre_name, director_name)
    else:
        process_actor(date_time, language_name, genre_name, None)


def process_actor(date_time, language_name, genre_name, director_name):
    global param_actor, starring, supporting_actors
    if starring is not None:
        for actor_name in starring:
            param_actor.append((actor_name,))
            process_review(date_time, language_name, genre_name, director_name, actor_name)
    elif supporting_actors is not None:
        for actor_name in supporting_actors:
            param_actor.append((actor_name,))
            process_review(date_time, language_name, genre_name, director_name, actor_name)
    else:
        process_review(date_time, language_name, genre_name, director_name, None)


def process_review(date_time, language_name, genre_name, director_name, actor_name):
    global param_actor, param_movie, starring, supporting_actors, movie_id, movie_name, \
        average_rating, studio, mpaa_rating, runtime, description, rank
    cursor.execute(query_review, (movie_id,))
    results = cursor.fetchall()
    if results is not None:
        for row in results:
            if date_time is not None:
                param_movie.append((movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description,
                                    rank, date_time.tm_year, date_time.tm_mon, date_time.tm_mday,
                                    language_name, genre_name, row[0], director_name, actor_name))
            else:
                param_movie.append((movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description,
                                    rank, None, None, None,
                                    language_name, genre_name, row[0], director_name, actor_name))


def execute_insert_many():
    global param_director, param_actor, param_time, param_movie
    cursor.executemany(sql_director, param_director)
    cursor.executemany(sql_actor, param_actor)
    cursor.executemany(sql_time, param_time)
    cursor.executemany(sql_movie, param_movie)
    db.commit()


def clear_params():
    global param_director, param_actor, param_time, param_movie
    param_director = []
    param_actor = []
    param_time = []
    param_movie = []


def load_data(filename):
    global param_director, param_actor, param_time, param_movie,\
        movie_id, movie_name, languages, directors, starring, supporting_actors, \
        release_date, average_rating, studio, mpaa_rating, genres, runtime, description, rank
    with codecs.open(filename, encoding='utf-8') as f:
        nrow = 0
        for line in f:
            nrow += 1
            if line == u'[\n' or line == u']':
                continue
            data = json.loads(line[:len(line) - 2])
            movie_id = data.get("oid")
            movie_name = data.get("name")
            languages = data.get("language")
            directors = data.get("director")
            starring = data.get("starring")
            supporting_actors = data.get("supporting_actors")
            release_date = data.get("dvd_release_date")
            average_rating = data.get("average_rating")
            studio = data.get("studio")
            mpaa_rating = data.get("mpaa_rating")
            genres = data.get("genres")
            runtime = data.get("runtime")
            description = data.get("desc")
            rank = data.get("rank")
            process_line_data()

            if nrow % 100 == 0:
                execute_insert_many()
                clear_params()
                print nrow, ' done!'


        # 将剩下的数据插入
        execute_insert_many()
        clear_params()
        print nrow, ' done!'


if __name__=="__main__":

    # recreate_mixed_table()

    # 打开数据库连接
    db = MySQLdb.connect("localhost", "root", "123456", "mixeddb")

    db.set_character_set('utf8')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    load_data('../data/amazon1.json')
    load_data('../data/amazon2.json')
    load_data('../data/amazon3.json')
    load_data('../data/amazon4.json')
    load_data('../data/amazon5.json')

    # cursor.execute(delete_review_col)

    db.commit()

    # 关闭数据库连接
    db.close()