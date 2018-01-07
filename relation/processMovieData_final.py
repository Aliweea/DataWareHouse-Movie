#!/usr/bin/python
# -*- coding: UTF-8 -*-

# 将t_director和t-actor的主键改为name


import codecs
import json
import re

from preprocess import *
from relation.createTable import *

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
(movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description, rank, year, month, day)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
"""

# SQL 插入tr_movie_language语句
sql_language = "INSERT IGNORE INTO tr_movie_language (movie_id, language_name) VALUES (%s, %s)"

# SQL 插入tr_movie_genre语句
sql_genre = "INSERT IGNORE INTO tr_movie_genre (movie_id, genre_name) VALUES (%s, %s)"

# SQL 插入t_director语句
sql_director = "INSERT IGNORE INTO t_director (director_name) VALUES (%s)"

# SQL 插入t_actor语句
sql_actor = "INSERT IGNORE INTO t_actor (actor_name) VALUES (%s)"

# SQL 插入tr_direct语句
sql_direct = "INSERT IGNORE INTO tr_direct (movie_id, director_name) VALUES (%s, %s)"

# SQL 插入tr_act语句
sql_act = "INSERT IGNORE INTO tr_act (movie_id, actor_name, is_star) VALUES (%s, %s, %s)"

# SQL 插入tr_cooperate语句
sql_cooperate = "INSERT IGNORE INTO tr_cooperate (director_name, actor_name, cooperate_times) VALUES (%s, %s, -1)"

# SQL 更新tr_cooperate
update_cooperate = """UPDATE tr_cooperate SET cooperate_times=cooperate_times+1 
WHERE director_name=%s and actor_name=%s"""

# --------------------- SQL END ------------------------


# -----------------  VARIABLE BEGIN --------------------

# 储存有sql语句的param元组的list
param_time = []
param_movie = []
param_language = []
param_genre = []
param_director = []
param_direct = []
param_actor = []
param_act = []
param_cooperate = []

# ------------------ VARIABLE END ----------------------


def execute_insert_many():
    global param_time, param_movie, param_language, param_genre, \
        param_director, param_direct, param_actor, param_act, param_cooperate
    cursor.executemany(sql_time, param_time)
    cursor.executemany(sql_movie, param_movie)
    cursor.executemany(sql_language, param_language)
    cursor.executemany(sql_genre, param_genre)
    cursor.executemany(sql_director, param_director)
    cursor.executemany(sql_actor, param_actor)
    cursor.executemany(sql_direct, param_direct)
    cursor.executemany(sql_act, param_act)
    cursor.executemany(sql_cooperate, param_cooperate)
    cursor.executemany(update_cooperate, param_cooperate)
    db.commit()


def clear_params():
    global param_time, param_movie, param_language, param_genre, \
        param_director, param_direct, param_actor, param_act, param_cooperate
    param_time = []
    param_movie = []
    param_language = []
    param_genre = []
    param_director = []
    param_direct = []
    param_actor = []
    param_act = []
    param_cooperate = []


# 最后读取]会报错
def load_data(filename):
    global param_time, param_movie, param_language, param_genre, \
        param_director, param_direct, param_actor, param_act
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

            if release_date is not None:
                date_time = time.strptime(release_date, '%B %d, %Y')
                season = getSeason(date_time.tm_mon)
                if (
                date_time.tm_year, date_time.tm_mon, date_time.tm_mday, season, date_time.tm_wday) not in param_time:
                    param_time.append(
                        (date_time.tm_year, date_time.tm_mon, date_time.tm_mday, season, date_time.tm_wday))

                param_movie.append((movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description,
                                    rank, date_time.tm_year, date_time.tm_mon, date_time.tm_mday))
            else:
                param_movie.append((movie_id, movie_name, average_rating, studio, mpaa_rating, runtime, description,
                                    rank, None, None, None))

            pattern = re.compile(RE_NAME)

            if languages is not None:
                languages = languages.split(", ")
                for language_name in languages:
                    sub_languages = pattern.findall(language_name)
                    for sub_language in sub_languages:
                        param_language.append((movie_id, sub_language))

            if genres is not None:
                for genre_name in genres:
                    sub_genres = pattern.findall(genre_name)
                    for sub_genre in sub_genres:
                        param_genre.append((movie_id, sub_genre))

            if directors is not None:
                for director_name in directors:
                    sub_directors = pattern.findall(director_name)
                    for sub_director in sub_directors:
                        param_director.append((sub_director,))
                        param_direct.append((movie_id, sub_director))

            if starring is not None:
                for star_name in starring:
                    sub_stars = pattern.findall(star_name)
                    for sub_star in sub_stars:
                        param_actor.append((sub_star,))
                        param_act.append((movie_id, sub_star, IS_STAR))

            if supporting_actors is not None:
                for actor_name in supporting_actors:
                    sub_actors = pattern.findall(actor_name)
                    for sub_actor in sub_actors:
                        param_actor.append((sub_actor,))
                        param_act.append((movie_id, sub_actor, NOT_STAR))

            if directors is not None:
                for director_name in directors:
                    sub_directors = pattern.findall(director_name)
                    for sub_director in sub_directors:
                        if starring is not None:
                            for star_name in starring:
                                sub_stars = pattern.findall(star_name)
                                for sub_star in sub_stars:
                                    param_cooperate.append((sub_director, sub_star))

                        if supporting_actors is not None:
                            for actor_name in supporting_actors:
                                sub_actors = pattern.findall(actor_name)
                                for sub_actor in sub_actors:
                                    param_cooperate.append((sub_director, sub_actor))

            if nrow % 10000 == 0:
                execute_insert_many()
                clear_params()
                print nrow, ' done!'

        # 将剩下的数据插入
        execute_insert_many()
        clear_params()
        print nrow, ' done!'


if __name__=="__main__":

    recreate_relation_table()

    # 打开数据库连接
    db = MySQLdb.connect("localhost", "root", "123456", "datawarehouse")

    db.set_character_set('utf8')

    # 使用cursor()方法获取操作游标
    cursor = db.cursor()

    load_data('../data/amazon1.json')
    load_data('../data/amazon2.json')
    load_data('../data/amazon3.json')
    load_data('../data/amazon4.json')
    load_data('../data/amazon5.json')

    cursor.execute(sql_cooperate)

    db.commit()

    # 关闭数据库连接
    db.close()