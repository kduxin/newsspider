import requests as req
import re

sess = req.Session()
param = {
    'username':'duxin@cl.rcast.u-tokyo.ac.jp',
    'password':'a19960407'
}
url = 'https://www.bloomberg.com/feeds/bbiz/sitemap_2015_11.xml'
resp = sess.post(url, data=param, verify=False)
xml = str(resp.content)

sitemaps = """
Sitemap: https://www.bloomberg.com/feeds/bbiz/sitemap_index.xml
Sitemap: https://www.bloomberg.com/feeds/bpol/sitemap_index.xml
Sitemap: https://www.bloomberg.com/feeds/businessweek/sitemap_index.xml
Sitemap: https://www.bloomberg.com/feeds/technology/sitemap_index.xml
Sitemap: https://www.bloomberg.com/bcom/sitemaps/people-index.xml
Sitemap: https://www.bloomberg.com/bcom/sitemaps/private-companies-index.xml
Sitemap: https://www.bloomberg.com/feeds/bbiz/sitemap_securities_index.xml
Sitemap: https://www.bloomberg.com/billionaires/live-data/sitemap.xml
Sitemap: https://www.bloomberg.com/feeds/bbiz/sitemap_news.xml
Sitemap: https://www.bloomberg.com/feeds/curated/feeds/graphics_news.xml
Sitemap: https://www.bloomberg.com/feeds/curated/feeds/graphics_sitemap.xml
"""

sitemaps = re.findall(r'(?=https://).+?(?<=xml)', sitemaps)

n_retries = []
for year in [2015,2016,2017,2018]:
    for month in range(1,13):
        url = 'https://www.bloomberg.com/feeds/businessweek/sitemap_%d_%d.xml'%(year,month)
        resp = sess.get(url, verify=True)
        xml = str(resp.content)
        results = re.findall(r'(?=https://).+?(?=</loc>)', string=xml)
        n_retries.append((year,month,len(results)))
        print('# of entries in {}/{}: {}'.format(year,month,len(results)))
