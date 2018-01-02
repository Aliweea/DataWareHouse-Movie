#!/usr/bin/python
# -*- coding: UTF-8 -*-

# 获取tr_direct表中director_id字段的方法
# 之前：处理数据时通过list.index()查找
# 之后：处理数据时保存director_name，之后通过sql语句查找director_id

import codecs
import json

from preprocess import *
from relation.createTable import *

# ----------------- CONSTANT BEGIN ----------------

IS_STAR = 1
NOT_STAR = 0

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
sql_direct = """INSERT IGNORE INTO tr_direct (movie_id, director_id) 
SELECT %s, td.director_id FROM t_director td WHERE director_name=%s"""

# SQL 插入tr_direct语句
sql_act = """INSERT IGNORE INTO tr_act (movie_id, is_star, actor_id) 
SELECT %s, %s, ta.actor_id FROM t_actor ta WHERE actor_name=%s"""

# # SQL 插入tr_cooperate语句
# sql_cooperate = "INSERT INTO tr_cooperate (director_id, actor_id, cooperate_times) VALUES (%s, %s, %s)"

# SQL 插入tr_cooperate语句
sql_cooperate = """INSERT IGNORE INTO tr_cooperate (director_id, actor_id, cooperate_times) 
SELECT * FROM (SELECT td.director_id, ta.actor_id 
                FROM t_director td JOIN t_actor ta JOIN tmp_cooperate tc
                WHERE td.director_name=tc.director_name and ta.actor_name=tc.actor_name) as tmp"""

# SQL 创建tmp_coopearte
create_tmp_coopearte = """CREATE TABLE `tmp_cooperate` (
                              `director_name` varchar(100) NOT NULL,
                              `actor_name` varchar(250) NOT NULL,
                              `cooperate_times` INT(4) NOT NULL,
                              PRIMARY KEY (`director_name`, `actor_name`),
                              UNIQUE KEY `actor_name_UNIQUE` (`actor_name`))
                            ENGINE = InnoDB
                            DEFAULT CHARACTER SET = utf8;"""

# SQL 插入tr_cooperate语句
sql_tmp_cooperate = "INSERT IGNORE INTO tmp_cooperate (director_name, actor_name, cooperate_times) VALUES (%s, %s, 0)"

# SQL 更新tmp_cooperate
update_tmp_cooperate = """UPDATE tmp_cooperate SET cooperate_times=cooperate_times+1 
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
    cursor.executemany(sql_tmp_cooperate, param_cooperate)
    cursor.executemany(update_tmp_cooperate, param_cooperate)
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

            if languages is not None:
                languages = languages.split(", ")
                for language_name in languages:
                    param_language.append((movie_id, language_name))

            if genres is not None:
                for genre_name in genres:
                    param_genre.append((movie_id, genre_name))

            if directors is not None:
                for director_name in directors:
                    param_director.append((director_name,))
                    param_direct.append((movie_id, director_name))

            if starring is not None:
                for actor_name in starring:
                    param_actor.append((actor_name,))
                    param_act.append((movie_id, IS_STAR, actor_name))

            if supporting_actors is not None:
                for actor_name in supporting_actors:
                    param_actor.append((actor_name,))
                    param_act.append((movie_id, NOT_STAR, actor_name))

            if directors is not None:
                for director_name in directors:
                    if starring is not None:
                        for actor_name in starring:
                            param_cooperate.append((director_name, actor_name))

                    if supporting_actors is not None:
                        for actor_name in supporting_actors:
                            param_cooperate.append((director_name, actor_name))

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

    cursor.execute("DROP TABLE IF EXISTS tmp_cooperate")

    cursor.execute(create_tmp_coopearte)

    load_data('../data/amazon1.json')
    load_data('../data/amazon2.json')
    load_data('../data/amazon3.json')
    load_data('../data/amazon4.json')
    load_data('../data/amazon5.json')

    cursor.execute(sql_cooperate)

    cursor.execute("DROP TABLE IF EXISTS tmp_cooperate")

    db.commit()

    # 关闭数据库连接
    db.close()