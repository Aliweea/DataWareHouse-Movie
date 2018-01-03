#!/usr/bin/python
# -*- coding: UTF-8 -*-

import MySQLdb

# create table t_time
create_time = """CREATE TABLE `mixeddb`.`t_time` (
  `year` INT(4) NOT NULL,
  `month` INT(2) NOT NULL,
  `day` INT(2) NOT NULL,
  `season` INT(1) NULL,
  `weekday` INT(1) NULL,
PRIMARY KEY (`year`, `month`, `day`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;"""

# create table t_movie
create_movie = """CREATE TABLE `mixeddb`.`t_movie` (
  `movie_id` bigint(20) NOT NULL AUTO_INCREMENT,
  `product_id` varchar(40) DEFAULT NULL,
  `movie_name` varchar(400) NOT NULL,
  `average_rating` float DEFAULT NULL,
  `studio` varchar(100) DEFAULT NULL,
  `mpaa_rating` varchar(100) DEFAULT NULL,
  `runtime` int(3) DEFAULT NULL,
  `description` text,
  `rank` int(10) DEFAULT NULL,
  `year` int(4) DEFAULT NULL,
  `month` int(2) DEFAULT NULL,
  `day` int(2) DEFAULT NULL,
  `language_name` varchar(100) DEFAULT NULL,
  `genre_name` varchar(45) DEFAULT NULL,
  `director_id` int(11) NOT NULL DEFAULT '0',
  `actor_id` int(11) NOT NULL DEFAULT '0',
  `review_id` int(11) NOT NULL DEFAULT '0',
  PRIMARY KEY (`movie_id`,`director_id`,`actor_id`,`review_id`),
  KEY `fk1_director_idx` (`director_id`),
  KEY `fk2_actor_idx` (`actor_id`),
  KEY `fk3_time_idx` (`year`,`day`,`month`),
  KEY `fk4_review_idx` (`review_id`),
  KEY `fk_movie_to_time` (`year`,`month`,`day`),
  CONSTRAINT `fk4_movie_to_review` 
    FOREIGN KEY (`review_id`) 
    REFERENCES `t_review` (`review_id`) 
    ON DELETE CASCADE 
    ON UPDATE CASCADE,
  CONSTRAINT `fk1_movie_to_director` 
    FOREIGN KEY (`director_id`) 
    REFERENCES `t_director` (`director_id`) 
    ON DELETE CASCADE 
    ON UPDATE CASCADE,
  CONSTRAINT `fk2_movie_to_actor` 
    FOREIGN KEY (`actor_id`) 
    REFERENCES `t_actor` (`actor_id`) 
    ON DELETE CASCADE 
    ON UPDATE CASCADE,
  CONSTRAINT `fk_movie_to_time` 
    FOREIGN KEY (`year`, `month`, `day`) 
    REFERENCES `t_time` (`year`, `month`, `day`) 
    ON DELETE SET NULL 
    ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

# create table t_director
create_director = """CREATE TABLE `mixeddb`.`t_director` (
  `director_id` INT NOT NULL AUTO_INCREMENT,
  `director_name` VARCHAR(150) NOT NULL,
  PRIMARY KEY (`director_id`),
  UNIQUE KEY `director_name_UNIQUE` (`director_name`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;"""

# create table t_actor
create_actor = """CREATE TABLE `mixeddb`.`t_actor` (
  `actor_id` INT NOT NULL AUTO_INCREMENT,
  `actor_name` VARCHAR(250) NOT NULL,
  PRIMARY KEY (`actor_id`),
  UNIQUE KEY `actor_name_UNIQUE` (`actor_name`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;"""

# create table t_user
create_user = """CREATE TABLE `mixeddb`.`t_user` (
`user_id` VARCHAR(40) NOT NULL,
`profilename` VARCHAR(100) NOT NULL,
PRIMARY KEY (`user_id`))
ENGINE = InnoDB
DEFAULT CHARACTER SET = utf8;"""

# create table t_review
create_review = """CREATE TABLE `t_review` (
  `review_id` int(11) NOT NULL AUTO_INCREMENT,
  `movie_id` VARCHAR(40) NOT NULL,
  `user_id` varchar(40) NOT NULL,
  `helpfulness` varchar(20) DEFAULT NULL,
  `score` float DEFAULT NULL,
  `summary` varchar(300) DEFAULT NULL,
  `text` text,
  `timestamp` bigint(16) DEFAULT NULL,
  PRIMARY KEY (`review_id`),
  KEY `fk1_review_idx` (`user_id`),
  CONSTRAINT `fk1_review` FOREIGN KEY (`user_id`) 
  REFERENCES `mixeddb`.`t_user` (`user_id`) 
  ON DELETE CASCADE 
  ON UPDATE CASCADE) 
ENGINE=InnoDB 
DEFAULT CHARSET=utf8;"""


def recreate_mixed_table():
    # 打开数据库连接
    db = MySQLdb.connect("localhost","root","123456","mixeddb" )
    # 使用cursor()方法获取操作游标
    cursor = db.cursor()
    # 删除表的顺序与创建表相反
    cursor.execute("DROP TABLE IF EXISTS t_movie")
    cursor.execute("DROP TABLE IF EXISTS t_time")
    cursor.execute("DROP TABLE IF EXISTS t_actor")
    cursor.execute("DROP TABLE IF EXISTS t_director")
    # cursor.execute("DROP TABLE IF EXISTS t_review")
    # cursor.execute("DROP TABLE IF EXISTS t_user")
    # 创建表
    # cursor.execute(create_user)
    # cursor.execute(create_review)
    cursor.execute(create_director)
    cursor.execute(create_actor)
    cursor.execute(create_time)
    cursor.execute(create_movie)

    # 关闭数据库连接
    db.close()


if __name__=="__main__":
    recreate_mixed_table()