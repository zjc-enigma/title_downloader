import json
import pandas as pd


log_path = "../data/all_0701"

fd = open(log_path)

json_list = []

for json_item in fd:
    json_list.append(json_item)


all_json = '[%s]' % ','.join(json_list)

all_data = pd.read_json(all_json)


print "uv mean %d " % all_data.prev_uv.mean(axis=0)
print "uv median %d " % all_data.prev_uv.median(axis=0)
print "pv mean %d " % all_data.prev_pv.mean(axis=0)
print "pv median %d " % all_data.prev_pv.median(axis=0)


