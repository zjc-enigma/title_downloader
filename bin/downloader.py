#coding:utf-8
import sys
#import pandas as pd
import json
import re
from functools import partial
from multiprocessing.pool import Pool
import urllib2
import urlparse
from bs4 import BeautifulSoup
sys.path.append("..")
from utils import myutils
myutils.set_ipython_encoding_utf8()


def multi_thread(handler, job_list, thread_num):
    p = Pool(thread_num)
    job_handler = partial(handler)
    # set chunksize = 1 to ensure the order
    return p.map(job_handler, job_list, chunksize=1)




def get_title_from_url(url):

    url = myutils.add_url_header(url)

    send_headers = {
            'User-Agent':'Mozilla/6.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'
        }
    try:
        req = urllib2.Request(url, headers=send_headers)
        content = urllib2.urlopen(req, timeout=20).read()
        content = BeautifulSoup(content, "lxml")
        title = content.title.get_text()

        print "GET TITLE"
        return title

    except Exception, e:
        print "NONE TITLE"
        return ""


if __name__ = "__main__":
    domain_list = open('../data/domain_list')
    #domain_regex = r"^(http|https)://[^/=?]*(sina.com|sohu.com|163.com|ifeng.com)"

    domain_regex =  r"^(http|https)://"
    urls = [ urllib2.unquote(json.loads(domain_item)['prev_url'].strip()) for domain_item in domain_list.readlines() ]
    start_urls = [ url for url in urls if re.search(domain_regex, url) ]

    domain_list.close()
    res = multi_thread(get_title_from_url, start_urls, 25)

