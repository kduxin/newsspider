import os
import sys
import requests
import json
import numpy as np
import pandas as pd
import re
import multiprocessing as mtp
import pymysql

def DbInitialize(db, user, passwd, host="localhost", port=3306):
    # db initialization
    conn = pymysql.connect(host=host,
                        port=port,
                        user=user,
                        passwd=passwd,
                        db=db)
    cur = conn.cursor()
    return conn, cur

conn, cur = DbInitialize(db='newsspider', user='duxin', passwd='')

months = [(year,month) for year in range(1991,2019) for month in range(1,13)]
info_list = []
for year,month in months:
    news_sm = json.loads(open('sitemap_bloomberg/feeds_bbiz_sitemap_{}_{}.json'.format(year,month), 'rt').read())
    info_list.append(news_sm)
info_list = [info for news_sm in info_list for info in news_sm]
info_list2 = [None] * len(info_list)
for i,info in enumerate(info_list):
    info_list2[i] = {
        'loc':info['loc'],
        'publishat':re.findall(r'\d{4}-\d{2}-\d{2}', info['loc'])[0],
        'lastmod':info['lastmod'].replace('T',' ').replace('Z',''),
        'changefreq':info['changefreq'],
        'priority':str(info['priority'])
        }
keys = list(info_list2[0].keys())
info_list2 = [list(info.values()) for info in info_list2]

qmarks = ', '.join(['%s'] * len(keys))
cur.executemany("insert into {} ({}) values({})".format("news_bloomberg_index", ','.join(keys), qmarks), info_list2)
conn.commit()

