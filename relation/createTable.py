#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb

# create table t_time
create_time = """CREATE TABLE `datawarehouse`.`t_time` (
                    `year` INT(4) NOT NULL,
                    `month` INT(2) NOT NULL,
                    `day` INT(2) NOT NULL,
                    `season` INT(1) NULL,
                    `weekday` INT(1) NULL,
                  PRIMARY KEY (`year`, `month`, `day`))
                  ENGINE = InnoDB
                  DEFAULT CHARACTER SET = utf8;"""

# create table t_movie
create_movie = """CREATE TABLE `datawarehouse`.`t_movie` (
                      `movie_id` VARCHAR(40) NOT NULL,
                      `movie_name` VARCHAR(400) NOT NULL,
                      `average_rating` FLOAT NULL,
                      `studio` VARCHAR(100) NULL,
                      `mpaa_rating` VARCHAR(100) NULL,
                      `runtime` INT(3) NULL,
                      `description` TEXT(4000) NULL,
                      `rank` INT(10) NULL,
                      `year` INT(4) NULL,
                      `month` INT(2) NULL,
                      `day` INT(2) NULL,
                      PRIMARY KEY (`movie_id`),
                      INDEX `fk_movie_to_time_idx` (`year` ASC, `month` ASC, `day` ASC),
                      CONSTRAINT `fk_movie_to_time`
                        FOREIGN KEY (`year` , `month` , `day`)
                        REFERENCES `datawarehouse`.`t_time` (`year` , `month` , `day`)
                        ON DELETE SET NULL
                        ON UPDATE NO ACTION)
                      ENGINE = InnoDB
                      DEFAULT CHARACTER SET = utf8;"""

# create table t_director
create_director = """CREATE TABLE `datawarehouse`.`t_director` (
                          `director_id` INT NOT NULL AUTO_INCREMENT,
                          `director_name` VARCHAR(150) NOT NULL,
                          PRIMARY KEY (`director_id`),
                          UNIQUE KEY `director_name_UNIQUE` (`director_name`))
                        ENGINE = InnoDB
                        DEFAULT CHARACTER SET = utf8;"""

# create table t_actor
create_actor = """CREATE TABLE `datawarehouse`.`t_actor` (
                      `actor_id` INT NOT NULL AUTO_INCREMENT,
                      `actor_name` VARCHAR(250) NOT NULL,
                      PRIMARY KEY (`actor_id`),
                      UNIQUE KEY `actor_name_UNIQUE` (`actor_name`))
                    ENGINE = InnoDB
                    DEFAULT CHARACTER SET = utf8;"""

# create table tr_movie_language
create_language = """CREATE TABLE `datawarehouse`.`tr_movie_language` (
                          `movie_id` VARCHAR(40) NOT NULL,
                          `language_name` VARCHAR(100) NOT NULL,
                          PRIMARY KEY (`movie_id`, `language_name`),
                          CONSTRAINT `fk_language`
                            FOREIGN KEY (`movie_id`)
                            REFERENCES `datawarehouse`.`t_movie` (`movie_id`)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE)
                        ENGINE = InnoDB
                        DEFAULT CHARACTER SET = utf8;"""

# create table tr_movie_genre
create_genre = """CREATE TABLE `datawarehouse`.`tr_movie_genre` (
                      `movie_id` VARCHAR(40) NOT NULL,
                      `genre_name` VARCHAR(45) NOT NULL,
                      PRIMARY KEY (`movie_id`, `genre_name`),
                      CONSTRAINT `fk_genre`
                        FOREIGN KEY (`movie_id`)
                        REFERENCES `datawarehouse`.`t_movie` (`movie_id`)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE)
                    ENGINE = InnoDB
                    DEFAULT CHARACTER SET = utf8;"""

# create table tr_direct
create_direct = """CREATE TABLE `datawarehouse`.`tr_direct` (
                      `movie_id` VARCHAR(40) NOT NULL,
                      `director_id` INT NOT NULL,
                      PRIMARY KEY (`movie_id`, `director_id`),
                      INDEX `fk2_direct_idx` (`director_id` ASC),
                      CONSTRAINT `fk1_direct`
                        FOREIGN KEY (`movie_id`)
                        REFERENCES `datawarehouse`.`t_movie` (`movie_id`)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE,
                      CONSTRAINT `fk2_direct`
                        FOREIGN KEY (`director_id`)
                        REFERENCES `datawarehouse`.`t_director` (`director_id`)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE)
                    ENGINE = InnoDB
                    DEFAULT CHARACTER SET = utf8;"""

# create table tr_act
create_act = """CREATE TABLE `datawarehouse`.`tr_act` (
                  `movie_id` VARCHAR(40) NOT NULL,
                  `actor_id` INT NOT NULL,
                  `is_star` INT(1) NOT NULL,
                  PRIMARY KEY (`movie_id`, `actor_id`, `is_star`),
                  INDEX `fk2_act_idx` (`actor_id` ASC),
                  CONSTRAINT `fk1_act`
                    FOREIGN KEY (`movie_id`)
                    REFERENCES `datawarehouse`.`t_movie` (`movie_id`)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE,
                  CONSTRAINT `fk2_act`
                    FOREIGN KEY (`actor_id`)
                    REFERENCES `datawarehouse`.`t_actor` (`actor_id`)
                    ON DELETE CASCADE
                    ON UPDATE CASCADE)
                ENGINE = InnoDB
                DEFAULT CHARACTER SET = utf8;"""

# create table tr_cooperate
create_coopearte = """CREATE TABLE `datawarehouse`.`tr_cooperate` (
                          `director_id` INT NOT NULL,
                          `actor_id` INT NOT NULL,
                          `cooperate_times` INT(3) NULL,
                          PRIMARY KEY (`director_id`, `actor_id`),
                          INDEX `fk2_coopearte_idx` (`actor_id` ASC),
                          CONSTRAINT `fk1_cooperate`
                            FOREIGN KEY (`director_id`)
                            REFERENCES `datawarehouse`.`t_director` (`director_id`)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE,
                          CONSTRAINT `fk2_coopearte`
                            FOREIGN KEY (`actor_id`)
                            REFERENCES `datawarehouse`.`t_actor` (`actor_id`)
                            ON DELETE CASCADE
                            ON UPDATE CASCADE)
                        ENGINE = InnoDB
                        DEFAULT CHARACTER SET = utf8;"""

# create table t_user
create_user = """CREATE TABLE `datawarehouse`.`t_user` (
                  `user_id` VARCHAR(40) NOT NULL,
                  `profilename` VARCHAR(100) NOT NULL,
                  PRIMARY KEY (`user_id`))
                  ENGINE = InnoDB
                  DEFAULT CHARACTER SET = utf8;"""

# create table t_review
create_review = """CREATE TABLE `datawarehouse`.`t_review` (
                      `review_id` INT NOT NULL AUTO_INCREMENT,
                      `user_id` VARCHAR(40) NOT NULL,
                      `movie_id` VARCHAR(40) NOT NULL,
                      `helpfulness` VARCHAR(20) NULL,
                      `score` FLOAT NULL,
                      `summary` VARCHAR(300) NULL,
                      `text` TEXT(4000) NULL,
                      `timestamp` BIGINT(16) NULL,
                      PRIMARY KEY (`review_id`),
                      INDEX `fk1_review_idx` (`user_id` ASC),
                      INDEX `fk2_review_idx` (`movie_id` ASC),
                      CONSTRAINT `fk1_review`
                        FOREIGN KEY (`user_id`)
                        REFERENCES `datawarehouse`.`t_user` (`user_id`)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE,
                      CONSTRAINT `fk2_review`
                        FOREIGN KEY (`movie_id`)
                        REFERENCES `datawarehouse`.`t_movie` (`movie_id`)
                        ON DELETE CASCADE
                        ON UPDATE CASCADE)
                    ENGINE = InnoDB
                    DEFAULT CHARACTER SET = utf8;"""


def recreate_relation_table():
    # 打开数据库连接
    db = MySQLdb.connect("localhost","root","123456","datawarehouse" )
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # 删除表的顺序与创建表相反
    cursor.execute("DROP TABLE IF EXISTS t_review")
    cursor.execute("DROP TABLE IF EXISTS t_user")
    cursor.execute("DROP TABLE IF EXISTS tr_cooperate")
    cursor.execute("DROP TABLE IF EXISTS tr_act")
    cursor.execute("DROP TABLE IF EXISTS t_actor")
    cursor.execute("DROP TABLE IF EXISTS tr_direct")
    cursor.execute("DROP TABLE IF EXISTS t_director")
    cursor.execute("DROP TABLE IF EXISTS tr_movie_genre")
    cursor.execute("DROP TABLE IF EXISTS tr_movie_language")
    cursor.execute("DROP TABLE IF EXISTS t_movie")
    cursor.execute("DROP TABLE IF EXISTS t_time")
    # 创建表
    cursor.execute(create_time)
    cursor.execute(create_movie)
    cursor.execute(create_language)
    cursor.execute(create_genre)
    cursor.execute(create_director)
    cursor.execute(create_direct)
    cursor.execute(create_actor)
    cursor.execute(create_act)
    cursor.execute(create_coopearte)
    # cursor.execute(create_user)
    # cursor.execute(create_review)

    # 关闭数据库连接
    db.close()



if __name__=="__main__":
    recreate_relation_table()