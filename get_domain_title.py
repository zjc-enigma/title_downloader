#coding:utf-8
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup
import urllib2
import urlparse
import sys
from functools import partial
from multiprocessing.pool import Pool

if len(sys.argv) < 2:
    print "check args num"
    sys.exit(-1)

model_name = sys.argv[1]
model_industry = sys.argv[2]

keywords_file = model_industry + "_keywords"
keywords_list = pd.read_csv(keywords_file,
                            sep="\t",
                            header=None)[0]

model_file = "feature_select/feature_weight_" + model_name

known_domain_file = "known_domain_info"
known_domain_data = pd.read_csv(known_domain_file, sep="")


model_data = pd.read_csv(model_file,
                         sep=" ",
                         header=None)


model_data.columns = ["url",
                      "poscnt",
                      "totalpos",
                      "posrate",
                      "negcnt",
                      "totalneg",
                      "negrate",
                      "lift",
                      "score"]

model_data = model_data[["url", "score"]]

model_data['domain'] = model_data.url.apply(lambda x: urlparse.urlparse(x).netloc)



def get_title_from_url(url):

    url = "http://"+url
    wfd = open("error_file", 'a')
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
        print e
        wfd.write(url + "\n")
        return ""

def multi_thread(handler, job_list, thread_num):
    p = Pool(thread_num)
    job_handler = partial(handler)
    # set chunksize = 1 to ensure the order
    return p.map(job_handler, job_list, chunksize=1)

unknown_domain_data = model_data[-model_data['domain'].isin(known_domain_data['domain'])]

unknown_domain_pos = unknown_domain_data.drop_duplicates('domain', take_last=True)

#unknown_titles = unknown_domain_pos.domain.apply(get_title_from_url)
unknown_titles = pd.DataFrame(multi_thread(get_title_from_url, unknown_domain_pos.domain, 25))

def drop_index(data_frame):
    data_frame.reset_index(drop=True, inplace=True)
    return data_frame

new_domain_data = pd.concat([drop_index(pd.DataFrame(unknown_domain_pos.domain)),
                             drop_index(unknown_titles)], axis=1)
#                             unknown_domain_pos.poscnt,
#                             unknown_domain_neg.negcnt], axis=1)
new_domain_data.columns = ["domain",
                           "title"]
#                           "poscnt",
#                           "negcnt"]
new_domain_data = new_domain_data[new_domain_data.title != ""]

all_domain_data = known_domain_data.append(new_domain_data)

all_domain_data.to_csv(known_domain_file, sep="", encoding='utf-8', index=None)

filtered_domain_data = all_domain_data[all_domain_data.title.str.contains('|'.join(keywords_list))]
filtered_domain_file = model_industry + "_filtered_domain_info"
filtered_domain_data.to_csv(filtered_domain_file, sep='', encoding='utf-8', index=None)



new_model_data = pd.merge(model_data, filtered_domain_data, on='domain', how='inner')
new_model_data = new_model_data.drop_duplicates('url', take_last=True)

new_model_data = new_model_data[['url', 'score', 'domain']]

thresh_model_file = model_name + "/url_model_weight_" + model_name
thresh_model_data = pd.read_csv(thresh_model_file,
                                sep="\t",
                                header=None)

thresh_model_data.columns = ['url', 'score']
thresh_model_data['domain'] = thresh_model_data.url.apply(lambda x: urlparse.urlparse(x).netloc)


new_model_data = new_model_data.append(thresh_model_data)
new_model_data = new_model_data.drop_duplicates('url', take_last=True)

black_domain_file = model_industry + "_black_domain_list"
black_domain_data = pd.read_csv(black_domain_file,
                                sep="\t")



new_model_data = new_model_data[~new_model_data['domain'].isin(black_domain_data['domain'])]
#result_data = pd.merge(result_data, filter_data, on='domain', how='outer')
new_model_data = new_model_data[['url', 'score']]
new_model_file = model_name + "/url_model_weight_" + model_name
new_model_data.to_csv(new_model_file,
              sep='\t',
              encoding='utf-8',
              header=None,
              index=None)
