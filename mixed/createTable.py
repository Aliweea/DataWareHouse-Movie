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
  `movie_id` varchar(40) DEFAULT NULL,
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
  `languages` varchar(500) DEFAULT NULL,
  `genres` varchar(500) DEFAULT NULL,
  `director_name` VARCHAR(200) DEFAULT NULL,
  `actor_name` VARCHAR(200) DEFAULT NULL,
  `is_star` INT(1) DEFAULT 0,
  PRIMARY KEY (`movie_id`),
  KEY `fk_movie_to_time` (`year`,`month`,`day`),
  CONSTRAINT `fk_movie_to_time` 
    FOREIGN KEY (`year`, `month`, `day`) 
    REFERENCES `t_time` (`year`, `month`, `day`) 
    ON DELETE SET NULL 
    ON UPDATE NO ACTION
) ENGINE=InnoDB DEFAULT CHARSET=utf8;
"""

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
    # cursor.execute("DROP TABLE IF EXISTS t_review")
    # cursor.execute("DROP TABLE IF EXISTS t_user")
    # 创建表
    # cursor.execute(create_user)
    # cursor.execute(create_review)
    cursor.execute(create_time)
    cursor.execute(create_movie)

    # 关闭数据库连接
    db.close()


if __name__=="__main__":
    recreate_mixed_table()