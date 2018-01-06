#!/usr/bin/python
# -*- coding: UTF-8 -*-

import codecs
import json

from relation.preprocess import *
from mixed.createTable import *

# ----------------- CONSTANT BEGIN ----------------

IS_STAR = 1
NOT_STAR = 0
RE_NAME = r"^[a-zA-Z]{1}([a-zA-Z]|[.\s]|·){0,200}$"

# ----------------- CONSTANT END  -----------------

# ------------------- SQL BEGIN ---------------------------

# SQL 插入t_time语句
sql_time = "INSERT IGNORE INTO t_time (year, month, day, season, weekday) VALUES (%s, %s, %s, %s, %s)"

# SQL 插入t_movie语句
sql_movie = """INSERT IGNORE INTO t_movie
(movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description, rank, year, month, day,
languages, genres, directors, stars, actors)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
"""

# --------------------- SQL END ------------------------


# -----------------  VARIABLE BEGIN --------------------

# 储存有sql语句的param元组的list
param_time = []
param_movie = []

# ------------------ VARIABLE END ----------------------

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
    global param_director, param_movie,\
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

            if languages is not None:
                str_languag = ''
                languages = languages.split(", ")
                for language_name in languages:
                    str_languag += ',' + language_name
                languages = str_languag

            if genres  is not None:
                str_genre = ''
                for genre_name in genres:
                    str_genre += ',' + genre_name
                genres = str_genre

            if directors is not None:
                str_director = ''
                for director_name in directors:
                    str_director += ',' + director_name
                directors = str_director

            if starring is not None:
                str_star = ''
                for star_name in starring:
                    str_star += ',' + star_name
                starring = str_star

            if supporting_actors is not None:
                str_actor = ''
                for actor_name in supporting_actors:
                    str_actor += ',' + actor_name
                supporting_actors = str_actor

            if release_date is not None:
                date_time = time.strptime(release_date, '%B %d, %Y')
                season = getSeason(date_time.tm_mon)
                param_time.append((date_time.tm_year, date_time.tm_mon, date_time.tm_mday,
                                   season, date_time.tm_wday))
                param_movie.append((movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description,
                                    rank, date_time.tm_year, date_time.tm_mon, date_time.tm_mday,
                                    languages, genres, directors, starring, supporting_actors))
            else:
                param_movie.append((movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description,
                                    rank, None, None, None,
                                    languages, genres, directors, starring, supporting_actors))

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