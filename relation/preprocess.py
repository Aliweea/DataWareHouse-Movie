#!/usr/bin/python
# -*- coding: UTF-8 -*-

import time

def getSeason(month):
    season = 0
    if month == 3 or month == 4 or month == 5:
        season = 1
    elif month == 6 or month == 7 or month == 8:
        season = 2
    elif month == 9 or month == 10 or month == 11:
        season = 3
    elif month == 12 or month == 1 or month == 2:
        season = 4
    return season


def execute_sql(cursor, sql, param, table):
    start = time.clock()
    cursor.executemany(sql, param)
    end = time.clock()
    print "insert data into %s:" % table, end - start