#!/usr/bin/python
# -*- coding: UTF-8 -*-

import codecs
import json
import re

from relation.preprocess import *
from mixed.createTable import *

# ----------------- CONSTANT BEGIN ----------------

IS_STAR = 1
NOT_STAR = 0
RE_NAME = r"[a-zA-Z]{1}[a-zA-Z\s]{0,200}\b"

# ----------------- CONSTANT END  -----------------

# ------------------- SQL BEGIN ---------------------------

# SQL 插入t_time语句
sql_time = "INSERT IGNORE INTO t_time (year, month, day, season, weekday) VALUES (%s, %s, %s, %s, %s)"

# SQL 插入t_movie语句
sql_movie = """INSERT IGNORE INTO t_movie
(movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description, rank, year, month, day,
languages, genres, director_name, actor_name, is_star)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# --------------------- SQL END ------------------------


# -----------------  VARIABLE BEGIN --------------------

# 储存有sql语句的param元组的list
param_time = []
param_movie = []

movie_id = ''
movie_name = ''
languages = ''
release_date = ''
average_rating = ''
studio = ''
mpaa_rating = ''
genres = ''
runtime = ''
description = ''
rank = ''
directors = ''
starring = ''
supporting_actors = ''

pattern = re.compile(RE_NAME)

# ------------------ VARIABLE END ----------------------

def process_line_data():
    global directors, pattern
    if directors is not None:
        for director_name in directors:
            sub_directors = pattern.findall(director_name)
            for sub_director in sub_directors:
                process_actor(sub_director)
    else:
        process_actor(None)


def process_actor(director_name):
    global starring, supporting_actors, pattern
    if starring is not None:
        for star_name in starring:
            sub_stars = pattern.findall(star_name)
            for sub_star in sub_stars:
                process_date(director_name, sub_star, IS_STAR)

    if supporting_actors is not None:
        for actor_name in supporting_actors:
            sub_actors = pattern.findall(actor_name)
            for sub_actor in sub_actors:
                process_date(director_name, sub_actor, NOT_STAR)
    else:
        process_date(director_name, None, NOT_STAR)


def process_date(director_name, actor_name, is_star):
    global param_director, param_movie,\
        movie_id, movie_name, languages, release_date, average_rating, \
        studio, mpaa_rating, genres, runtime, description, rank
    if release_date is not None:
        date_time = time.strptime(release_date, '%B %d, %Y')
        season = getSeason(date_time.tm_mon)
        param_time.append((date_time.tm_year, date_time.tm_mon, date_time.tm_mday,
                           season, date_time.tm_wday))
        param_movie.append((movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description,
                            rank, date_time.tm_year, date_time.tm_mon, date_time.tm_mday,
                            languages, genres, director_name, actor_name, is_star))
    else:
        param_movie.append((movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description,
                            rank, None, None, None,
                            languages, genres, director_name, actor_name, is_star))


def execute_insert_many():
    global param_director, param_actor, param_time, param_movie
    cursor.executemany(sql_time, param_time)
    cursor.executemany(sql_movie, param_movie)
    db.commit()


def clear_params():
    global param_director, param_actor, param_time, param_movie
    param_time = []
    param_movie = []


def load_data(filename):
    global movie_id, movie_name, languages, directors, starring, supporting_actors, \
        release_date, average_rating, studio, mpaa_rating, genres, runtime, description, rank, pattern
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

            if languages is not None:
                str_languag = ''
                languages = languages.split(", ")
                for language_name in languages:
                    sub_languages = pattern.findall(language_name)
                    for sub_language in sub_languages:
                        str_languag += sub_language + ','
                languages = str_languag[:len(str_languag)-1]

            if genres  is not None:
                str_genre = ''
                for genre_name in genres:
                    sub_genres = pattern.findall(genre_name)
                    for sub_genre in sub_genres:
                        str_genre += sub_genre + ','
                genres = str_genre[:len(str_genre)-1]

            process_line_data()


            if nrow % 5000 == 0:
                execute_insert_many()
                clear_params()
                print nrow, ' done!'


        # 将剩下的数据插入
        execute_insert_many()
        clear_params()
        print nrow, ' done!'


if __name__=="__main__":

    recreate_mixed_table()

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

    db.commit()

    # 关闭数据库连接
    db.close()