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

quarters = [(year,quarter) for year in range(1997,2019) for quarter in range(1,5)][2:]
info_list = []
for year,quarter in quarters:
    news_sm = json.loads(open('sitemap_economist/sitemap-{}-Q{}.json'.format(year,quarter), 'rt').read())
    info_list.append(news_sm)
info_list = [info for news_sm in info_list for info in news_sm]
info_list2 = []
count_no_loc = 0
count_no_lastmod = 0
count_no_changefreq = 0
count_no_priority = 0
for i,info in enumerate(info_list):
    if 'loc' not in info:
        count_no_loc += 1
        continue
    if 'lastmod' not in info:
        count_no_lastmod +=1
        continue
    if 'changefreq' not in info:
        count_no_changefreq +=1
        info['changefreq'] = None
    if 'priority' not in info:
        count_no_priority += 1
        info['priority'] = None
    publishat_find = re.findall(r'\d{4}[/-]\d{2}[/-]\d{2}', info['loc'])
    if len(publishat_find) == 0:
        publishat = None
        print("# publishat not found: {}".format(info['loc']))
    else:
        publishat = publishat_find[0]
    info_list2.append({
        'loc':info['loc'],
        'publishat': publishat,
        'lastmod':info['lastmod'].replace('T',' ').replace('Z',''),
        'changefreq':info['changefreq'],
        'priority':str(info['priority'])
        })
print("Loss of loc: {}".format(count_no_loc))
print("Loss of lastmod: {}".format(count_no_lastmod))
print("Loss of changefreq: {}".format(count_no_changefreq))
print("Loss of priority: {}".format(count_no_priority))
keys = list(info_list2[0].keys())
info_list2 = [list(info.values()) for info in info_list2]

qmarks = ', '.join(['%s'] * len(keys))
cur.executemany("insert into {} ({}) values({})".format("news_economist_index", ','.join(keys), qmarks), info_list2)
conn.commit()

