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
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities
from selenium.webdriver.firefox.firefox_binary import FirefoxBinary
import random

# logger initialization
def LoggerInitialize(logfile, logger_name='main_logger'):
    logger = logging.getLogger(logger_name)
    logger.setLevel(logging.INFO)
    logger.addHandler(logging.FileHandler(filename=logfile, mode='a')) 
    logger.addHandler(logging.StreamHandler())
    return logger

# selenium initialization
def SeleniumInitialize(set_headless=False, allow_image=False, allow_css=True, allow_flash=False, allow_js=True, binary_path=None):
    firefox_profile = webdriver.FirefoxProfile()
    firefox_options = webdriver.FirefoxOptions()
    cap = DesiredCapabilities().FIREFOX
    cap["marionette"] = True
    if True==set_headless:
        firefox_options.set_headless()
    if False==allow_image:
        firefox_profile.set_preference('permissions.default.image', 2)
    if False==allow_css:
        firefox_profile.set_preference('permissions.default.stylesheet', 2)
    if False==allow_flash:
        firefox_profile.set_preference('dom.ipc.plugins.enabled.libflashplayer.so', 'false')
    if False==allow_js:
        firefox_profile.set_preference('javascript.enabled', 'false')
    firefox_profile.update_preferences()
    if binary_path is not None:
        binary = FirefoxBinary(binary_path)
        driver = webdriver.Firefox(capabilities=cap, firefox_profile=firefox_profile, options=firefox_options, firefox_binary=binary)
    else:
        driver = webdriver.Firefox(capabilities=cap, firefox_profile=firefox_profile, options=firefox_options)
    return driver

# login
def Login(driver):
    driver.get('https://login.bloomberg.com/')
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "email"))).send_keys('duxin@cl.rcast.u-tokyo.ac.jp')
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "button-label")))
    time.sleep(3)
    driver.find_element_by_class_name('button-label').click()
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.NAME, "password"))).send_keys('a19960407')
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "button-label")))
    time.sleep(3)
    driver.find_element_by_class_name('button-label').click()
    time.sleep(3)
    if "Sign Out" in driver.find_element_by_tag_name('html').text:
        logger.info("{} # Login succeeded".format(time.ctime()))
    else:
        logger.error("{} # Login failed".format(time.ctime()))
        raise ValueError("Login Failed!")

def DbInitialize(db, user, passwd, host="localhost", port=3306):
    # db initialization
    conn = pymysql.connect(host=host,
                        port=port,
                        user=user,
                        passwd=passwd,
                        db=db)
    cur = conn.cursor()
    return conn, cur

def GetTaskFromDb():
    cur.execute("select ")

def CrawlByInfo(info, save_pagesource=True):
    id,loc,lastmod,changefreq,priority = info['id'],info['loc'],info['lastmod'],info['changefreq'],info['priority']
    # resp = sess.get(loc, headers=headers, verify=True)
    driver.get(loc)
    # if 200 != resp.status_code:
    #     logger.error("{} # Response error. CODE: {:>4}. MESSAGE: {}".format(time.ctime(), resp.status_code, resp.reason))
    #     continue
    text = driver.find_element_by_tag_name('html').text
    if True==save_pagesource:
        pagesource = driver.page_source
    else:
        pagesource = None
    
    # insert into database
    entry = {'id':id,
            'loc':loc,
            'publishat':re.findall(r'\d{4}-\d{2}-\d{2}', loc)[0],
            'lastmod':lastmod,
            'changefreq':changefreq,
            'priority':priority,
            'pagesource':pagesource,
            'text':text}
    qmarks = ', '.join(['%s'] * len(entry))

    if "Welcome, duxin" not in text:
        entry['error_type'] = "'Welcome, duxin' not found!"
        logger.error("{} # 'Welcome, duxin' not found".format(time.ctime()))
        cur.execute("insert into {} ({}) values({})".format(error_table, ','.join(entry.keys()), qmarks), list(entry.values()))
        raise ValueError("'Welcome, duxin' not found!")
    
    cur.execute("insert into {} ({}) values({})".format(table, ','.join(entry.keys()), qmarks), list(entry.values()))
    cur.execute("update news_bloomberg_index set status=1 where id=%d"%(id))
    return len(text), len(pagesource)

def CrawlByMonths(months, save_pagesource):
    for year,month in months:
        news_sm = json.loads(open('sitemap_bloomberg/feeds_bbiz_sitemap_{}_{}.json'.format(year,month), 'rt').read())

        for i,info in enumerate(news_sm):
            len_text, len_source = CrawlByInfo(info, save_pagesource)

            if 0 == (i+1) % 1:
                conn.commit()
            logger.info("{} # Inserted news. ID: {:>6d}. LENGTH: {:>6d}".format(time.ctime(), cur.lastrowid, len_text))
            if 0 == (i+1) % 10:
                logger.info("{} # Current progress: {}-{}, {:4}/{:4}".format(time.ctime(), year, month, i+1, len(news_sm)))
            
            time.sleep(random.random()*5)



if __name__=='__main1__':
    db, table = 'newsspider', 'news_bloomberg_copy'
    logger = LoggerInitialize('news_bloomberg.log')
    driver = SeleniumInitialize(set_headless=True)
    Login(driver)
    conn, cur = DbInitialize(db=db, user='root', passwd="123456", host="localhost")
    months = [(year,month) for year in range(2010,2019) for month in range(1,13)]
    try:
        CrawlByMonths(months, True)
    except Exception as e:
        logger.exception("Got unexpected error")

if __name__=='__main__':
    db, table, error_table = 'newsspider', 'news_bloomberg', 'news_bloomberg_error'
    logger = LoggerInitialize('news_bloomberg.log')
    driver = SeleniumInitialize(set_headless=True, binary_path="/home/duxin/bin/firefox/firefox")
    Login(driver)
    conn, cur = DbInitialize(db=db, user='duxin', passwd="", host="localhost")
    # prepare info_list
    cmd = "select id,loc,lastmod,changefreq,priority from news_bloomberg_index " \
        "where status=0 and id not in (select * from (select id from news_bloomberg as tb1) as tb2) " \
        "and lastmod between {} and {}".format("'2010-01-01'", "'2020-12-31'")
    cur.execute(cmd)
    info_list = cur.fetchall()
    # crawling
    flag = 0
    for i,(id,loc,lastmod,changefreq,priority) in enumerate(info_list):
        try:
            info = {'id':id,
                    'loc':loc,
                    'lastmod':lastmod,
                    'changefreq':changefreq,
                    'priority':priority}
            len_text, len_source = CrawlByInfo(info, True)
            if 0 == (i+1) % 10:
                conn.commit()
            logger.info("{} # Inserted news. ID: {:>6d}. SOURCE/TEXT LENGTH: {:>7d}/{:>6d}".format(time.ctime(), cur.lastrowid, len_source, len_text))
            if 0 == (i+1) % 100:
                logger.info("{} # Current progress: {}, {:4}/{:4}".format(time.ctime(), info['lastmod'], i+1, len(info_list)))
            if 0 == (i+1) % 300:
                logger.info("{} # Sleep for a while... (3min)".format(time.ctime()))
                time.sleep(180)
            
            time.sleep(random.random()*5)
            flag = 0
        except Exception as e:
            logger.exception("{} # Got unexpected error. Recent fails: {}".format(time.ctime(), flag+1))
            flag += 1
            if flag > 10:
                time.sleep(600)

        





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
