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

# cur.execute("select loc from news_bloomberg")
# locs = cur.fetchall()
# for loc in locs:
#     cur.execute("update news_bloomberg_index set status=1 where loc=%s", (loc[0],))
# conn.commit()

cur.execute("select id,loc from news_bloomberg_index where status=1")
ids = cur.fetchall()
for id,loc in ids:
    cur.execute("update news_bloomberg set id=%s where loc=%s", (id, loc))
conn.commit()

conn.close()