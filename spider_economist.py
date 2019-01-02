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
    driver.get('https://www.economist.com')
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "navigation__user-menu-link-caption")))
    time.sleep(3)
    driver.find_element_by_class_name('navigation__user-menu-link-caption').click()
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.CLASS_NAME, "navigation__user-menu-linklist-item")))
    time.sleep(3)
    driver.find_element_by_class_name('navigation__user-menu-linklist-item').click()
    WebDriverWait(driver, 10).until(EC.presence_of_element_located((By.TAG_NAME, "input")))
    inputs = driver.find_elements_by_tag_name('input')
    inputs[0].send_keys("duxin@cl.rcast.u-tokyo.ac.jp")
    inputs[1].send_keys("duxinKumiko")
    WebDriverWait(driver, 10).until(EC.element_to_be_clickable((By.ID, "submit-login")))
    driver.find_element_by_id("submit-login").click()

    try:
        WebDriverWait(driver, 10).until(EC.text_to_be_present_in_element((By.TAG_NAME, "html"), "You are now logged in"))
        print("Login succeeded")
        logger.info("{} # Login succeeded".format(time.ctime()))
    except:
        logger.error("{} # Login failed".format(time.ctime()))
        raise ValueError("Login failed!")

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

def None2Null(s):
    if s is None:
        return "null"
    else:
        return s

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
            'publishat':re.findall(r'\d{4}[/-]\d{2}[/-]\d{2}', loc)[0],
            'lastmod':None2Null(lastmod),
            'changefreq':None2Null(changefreq),
            'priority':None2Null(priority),
            'pagesource':pagesource,
            'text':text}

    flag = 0
    
    if "Welcome" not in text:
        if "We are unable to find the page" not in text:
            logger.error("{} # 404 : Page not found".format(time.ctime()))
            entry['error_type'] = "404 : Page not found!"
        else:
            flag += 1
            logger.error("{} # 'Welcome' not found".format(time.ctime()))
            entry['error_type'] = "'Welcome' not found!"
        qmarks = ', '.join(['%s'] * len(entry))
        cur.execute("replace into {} ({}) values({})".format(error_table, ','.join(entry.keys()), qmarks), list(entry.values()))
        conn.commit()
    if flag > 0:
        raise ValueError("Fingerprints not found!")
    
    qmarks = ', '.join(['%s'] * len(entry))
    cur.execute("insert into {} ({}) values({})".format(table, ','.join(entry.keys()), qmarks), list(entry.values()))
    cur.execute("update %s set status=1 where id=%d"%(index_table, id))
    return len(text), len(pagesource)


def RebootDriver(driver):
    try:
        driver.close()
        logger.info("{} # Succeeded to close current webdriver".format(time.ctime()))
    except Exception as e:
        logger.exception("{} # Failed to close current webdriver".format(time.ctime()))
    driver = SeleniumInitialize(set_headless=True, binary_path="/home/duxin/bin/firefox/firefox")
    logger.info("#"*60)
    logger.info("{} # New webdriver initialized".format(time.ctime()))
    return driver

    
if __name__=='__main__':
    db, table, error_table, index_table = 'newsspider', 'news_economist', 'news_economist_error', 'news_economist_index'
    logger = LoggerInitialize('news_economist.log')
    driver = SeleniumInitialize(set_headless=True, binary_path="/home/duxin/bin/firefox/firefox")
    Login(driver)
    conn, cur = DbInitialize(db=db, user='duxin', passwd="", host="localhost")
    # prepare info_list
    cmd = "select id,loc,lastmod,changefreq,priority from {} " \
        "where status=0 and id not in (select * from (select id from {} as tb1) as tb2) " \
        "and lastmod between {} and {}".format(index_table, table, "'2010-01-01'", "'2020-12-31'")
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
            logger.info("{} # Inserted news. ID: {:>6d}. SOURCE/TEXT LENGTH: {:>7d}/{:>6d}".format(time.ctime(), id, len_source, len_text))
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
            time.sleep(20)
            if flag > 5:
                time.sleep(300)
                driver = RebootDriver(driver)
                flag = 0
                while flag < 5:
                    try:
                        Login(driver)
                        break
                    except:
                        logger.exception("{} # Login failed. Retrying... {}".format(time.ctime(), flag))
                        flag += 1
                        time.sleep(10)
                        continue
                if flag == 5:
                    logger.error("{} $ Login failed. Process exits.".format(time.ctime()))
                    raise ValueError("Login failed. Process exits.")
                flag = 0

        
