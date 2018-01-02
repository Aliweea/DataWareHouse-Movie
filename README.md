# DataWareHouse-Movie
process and import data into mysql and hdfs



## 使用说明

#### 请按照以下要求配置环境：

1. python2.7
2. mysql_python-1.2.5
3. mysql-5.5.58

配置mysql时，将my-innodb-heavy-4G.ini复制并重命名为my.ini。

设置root的密码为123456.



#### 环境配置完成后，按照顺序执行以下文件：

##### 将数据导入mysql：

1. relation/processMovieData.py
2. relation/processReviewData.py

##### 将数据导入

1. mixed/processReview.py
2. mixed/processMovie.py

导入数据时请耐心等待。