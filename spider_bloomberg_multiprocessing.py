# crawl the news from "economists" from 2015-Q3 to 2017-Q4

import os
import sys
import requests
import json
import numpy as np
import pandas as pd
import re
import multiprocessing as mtp
import pymysql
import logging
import time
from selenium import webdriver
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
import random

# logger initialization
logger = logging.getLogger('main_logger')
logger.setLevel(logging.INFO)
logger.addHandler(logging.FileHandler(filename = 'news_bloomberg.log', mode = 'a')) 
logger.addHandler(logging.StreamHandler())
# selenium
# chrome_options = webdriver.ChromeOptions()
# prefs = {"profile.managed_default_content_settings.images": 2}
# chrome_options.add_experimental_option("prefs", prefs)
# driver = webdriver.Chrome(chrome_options=chrome_options)
firefox_profile = webdriver.FirefoxProfile()
# 不下载和加载图片
firefox_profile.set_preference('permissions.default.image', 2)
# 禁用 css
# firefox_profile.set_preference('permissions.default.stylesheet', 2)
# 禁用 flash
firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', 'false')
firefox_profile.update_preferences()
driver = webdriver.Firefox(firefox_profile=firefox_profile)
driver.get('https://login.bloomberg.com/')
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "email"))).send_keys('duxin@cl.rcast.u-tokyo.ac.jp')
WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "button-label")))
driver.find_element_by_class_name('button-label').click()
WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "password"))).send_keys('a19960407')
WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "button-label")))
driver.find_element_by_class_name('button-label').click()
time.sleep(5)
cookies = driver.get_cookies()
# sess initialization
sess = requests.Session()
sess.cookies.clear()
for cookie in cookies:
    sess.cookies.set(cookie['name'],cookie['value'])
sess.headers.clear()
# param = {
#     #'Referer': 'https://login.bloomberg.com/account',
#     'email':'duxin@cl.rcast.u-tokyo.ac.jp',
#     #'password':'a19960407'
# }
headers = {
    'referer':'https://login.bloomberg.com',
    'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36'
}
# sess.post('https://login.bloomberg.com/', data=param, headers=headers)
# open('test_html.html', 'wt').write(resp.text)

# db initialization
db = pymysql.connect(host="localhost",
                     user="root",
                     passwd="123456",
                     db="newsspider")
cur = db.cursor()

months = [(year,month) for year in range(2015,2019) for month in range(1,13)]

for year,month in months:
    news_sm = json.loads(open('sitemap_bloomberg/feeds_bbiz_sitemap_{}_{}.json'.format(year,month), 'rt').read())

    for i,info in enumerate(news_sm):
        loc, lastmod, changefreq, priority = info['loc'], info['lastmod'], info['changefreq'], info['priority']
        # resp = sess.get(loc, headers=headers, verify=True)
        driver.get(loc)
        # if 200 != resp.status_code:
        #     logger.error("{} # Response error. CODE: {:>4}. MESSAGE: {}".format(time.ctime(), resp.status_code, resp.reason))
        #     continue
        text = driver.find_element_by_tag_name('html').text
        
        # insert into database
        entry = {'loc':loc,
                'publishat':re.findall(r'\d{4}-\d{2}-\d{2}', loc)[0],
                'lastmod':lastmod.replace('T',' ').replace('Z',''),
                'changefreq':changefreq,
                'priority':str(priority),
                'content':text}
        qmarks = ', '.join(['%s'] * len(entry))
        cur.execute("insert into news_bloomberg_copy ({}) values({})".format(','.join(entry.keys()), qmarks), list(entry.values()))
        if 0 == (i+1) % 10:
            db.commit()
        logger.info("{} # Inserted news. ID: {:>6d}. LENGTH: {:>6d}".format(time.ctime(), cur.lastrowid, len(text)))
        if 0 == (i+1) % 100:
            logger.info("{} # Current progress: {}-{}, {:4}/{:4}".format(time.ctime(), year, month, i+1, len(news_sm)))
        
        time.sleep(random.random()*5+5)

webdriver.FirefoxOptions


# param = {
#     #'Referer': 'https://login.bloomberg.com/account',
#     'email':'duxin_be@outlook.com',
#     'password':'a19960407'
# }
# headers = {
#     'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36'
# }
# resp = sess.post('https://passport.mafengwo.cn/login/', data=param, headers=headers)
# open('test_html.html', 'wt').write(resp.text)



# # sess initialization
# sess = requests.Session()
# sess.cookies.clear()
# sess.headers.clear()
# cookies = ";".join([item["name"] + "=" + item["value"] + "" for item in cookies])
# # param = {
# #     #'Referer': 'https://login.bloomberg.com/account',
# #     'email':'duxin@cl.rcast.u-tokyo.ac.jp',
# #     #'password':'a19960407'
# # }
# headers = {
#     'referer':'https://login.bloomberg.com',
#     'User-Agent':'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/70.0.3538.110 Safari/537.36',
#     'Cookie':cookies
# }
# # sess.post('https://login.bloomberg.com/', data=param, headers=headers)
# # open('test_html.html', 'wt').write(resp.text)

# # db initialization
# db = pymysql.connect(host="localhost",
#                      user="root",
#                      passwd="123456",
#                      db="newsspider")
# cur = db.cursor()

# months = [(year,month) for year in range(2015,2019) for month in range(1,13)]

# for year,month in months:
#     news_sm = json.loads(open('sitemap_bloomberg/feeds_bbiz_sitemap_{}_{}.json'.format(year,month), 'rt').read())

#     for i,info in enumerate(news_sm):
#         loc, lastmod, changefreq, priority = info['loc'], info['lastmod'], info['changefreq'], info['priority']
#         resp = sess.get(loc, headers=headers, verify=True)
#         if 200 != resp.status_code:
#             logger.error("{} # Response error. CODE: {:>4}. MESSAGE: {}".format(time.ctime(), resp.status_code, resp.reason))
#             continue
#         text = resp.text
        
#         # insert into database
#         entry = {'loc':loc,
#                 'publishat':re.findall(r'\d{4}-\d{2}-\d{2}', loc)[0],
#                 'lastmod':lastmod.replace('T',' ').replace('Z',''),
#                 'changefreq':changefreq,
#                 'priority':str(priority),
#                 'content':text}
#         qmarks = ', '.join(['%s'] * len(entry))
#         cur.execute("insert into news_bloomberg_copy ({}) values({})".format(','.join(entry.keys()), qmarks), list(entry.values()))
#         if 0 == (i+1) % 10:
#             db.commit()
#         logger.info("{} # Inserted news. ID: {:>6d}. LENGTH: {:>6d}".format(time.ctime(), cur.lastrowid, len(text)))
#         if 0 == (i+1) % 100:
#             logger.info("{} # Current progress: {}-{}, {:4}/{:4}".format(time.ctime(), year, month, i+1, len(news_sm)))
#         time.sleep(5)
