# crawl the news from "economists" from 2015-Q3 to 2017-Q4

import os
import sys
import requests
import json
import numpy as np
import pandas as pd
import multiprocessing as mtp

sess = requests.Session()

print(list(os.walk('sitemap_economist/')))

quarters = [(2015,3), (2015,4), (2016,1), (2016,2), (2016,3), (2016,4), (2017,1), (2017,2), (2017,3), (2017,4)]

news = json.loads(open('sitemap_economist/sitemap-2016-Q2.json', 'rt').read())[0]

loc, lastmod, changefreq, priority = news.values()
resp = sess.get(loc)


with open('test_html.html','wt') as f:
    f.write(resp.text)