#coding:utf-8
import sys
#import pandas as pd
import json
import re
import time
from functools import partial
from multiprocessing.pool import Pool
from multiprocessing import Queue
import urllib2
import urlparse
from bs4 import BeautifulSoup
sys.path.append("..")
from utils import myutils
myutils.set_ipython_encoding_utf8()
begin = False

def multi_thread(handler, job_list, thread_num):
    p = Pool(thread_num)
    job_handler = partial(handler)
    # set chunksize = 1 to ensure the order
    # return p.map(job_handler, job_list, chunksize=1)
    return p.map(job_handler, job_list)




def get_title_from_url(url):

    url = myutils.add_url_header(url)

    send_headers = {
            'User-Agent':'Mozilla/6.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.1.6) Gecko/20091201 Firefox/3.5.6'
        }
    try:
        req = urllib2.Request(url, headers=send_headers)
        content = urllib2.urlopen(req, timeout=10).read()
        content = BeautifulSoup(content, "lxml")
        title = content.title.get_text()
        body = content.body.get_text()

        print "GET TITLE %s " % title
        # TODO: body extraction will got a lot of html tags , this is bad
        #return json.dumps({"title":title, "body":body, "url":url})
        #return {"url": url, "title": title}
        return title

    except Exception, e:
        print "NONE TITLE %s " % url
        return ""


def get_title_from_url_queue(queue, output_queue):

    while True:
        if queue.empty():
            time.sleep(20)
            if queue.empty():
                break

        else:
            url = queue.get(True)
            ret = get_title_from_url(url)
            if ret:
                output_queue.put(json.dumps(ret))

def write_to_disk(output_queue):

    res_file = "../data/thresh_titles-00002"
    wfd = open(res_file, 'w')

    while True:
        if output_queue.empty():
            time.sleep(20)
            if output_queue.empty():
                break

        else:
            msg = output_queue.get(True)
            if msg:
                wfd.write(msg + "\n")

    wfd.close()


if __name__ == "__main__":
    domain_list = open('../data/part-00002')
    #domain_regex = r"^(http|https)://[^/=?]*(sina.com|sohu.com|163.com|ifeng.com)"

    crawl_scale = 53227135
    domain_regex =  r"^(http|https)://"
    pv_thresh = 15
    uv_thresh = 5

    url_generator = ( urllib2.unquote(json.loads(domain_item)['prev_url'].strip()) for domain_item in domain_list if json.loads(domain_item)['prev_uv'] >= uv_thresh and json.loads(domain_item)['prev_pv'] >= pv_thresh)

    the_queue = Queue(maxsize=51200)
    output_queue = Queue(maxsize=51200)
    thread_num = 512

    #job_handler = partial(get_title_from_url)
    #res_title = []
    index = 1
    p = Pool(thread_num, get_title_from_url_queue, (the_queue, output_queue))
    wp = Pool(1, write_to_disk, (output_queue, ))

    while True:
        try:
            url = url_generator.next()
            if re.search(domain_regex, url):
                print ("number : %d url is processing" % index)
                print ("queue size : %d " % the_queue.qsize())
                the_queue.put(url)
                index += 1

            else:
                continue


        except Exception, e:
            print "reach the end of file"
            break

        #finally:
            #start_urls = [ url for url in start_urls if re.search(domain_regex, url) ]
            #res = multi_thread(get_title_from_url, start_urls, 32)
            #res = p.map(job_handler, start_urls)
            #tmp = [title for title in res if title != ""]
            #res_title = list(set(res_title + tmp))

    
    #res_file = "../data/crawled_titles-100to199"
    #res_title = list(set([title for title in output_queue]))

    domain_list.close()
    p.close()
    p.terminate()
    wp.terminate()
