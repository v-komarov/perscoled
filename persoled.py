#!/usr/bin/python3

import sys
import json
import pickle

from kafka import KafkaConsumer,TopicPartition
from scipy.spatial import distance


import conf as co



def comp(d1, d2):
    if distance.euclidean(d1,d2) <= 0.7:
        return True
    else:
        return False


line = sys.stdin.readline().strip()


if len(line) < 60:
    print("error")
    sys.exit()


topics = co.ka_topic
partition = co.ka_partition

consumer = KafkaConsumer(bootstrap_servers=co.ka_host, auto_offset_reset='earliest')
tp = TopicPartition(topics,partition)
consumer.assign([tp])
consumer.seek_to_end(tp)
lastOffset = consumer.position(tp)
consumer.seek_to_beginning(tp)





if lastOffset == 0:
    print("EVT_VISITOR_DETECTED:None:END\n")
    sys.exit()



else:
    d = json.loads(line)
    for m in consumer:
        if m.offset == lastOffset - 1:
            print("EVT_VISITOR_DETECTED:None:END\n")
            sys.exit()
        else:
            d1 = d["descriptor"]
            d2 = pickle.loads(m.value)["descriptor"]
            if comp(d1,d2):
                print("EVT_PERSON_DETECTED:%s:END\n" % pickle.loads(m.value)["id"])
                sys.exit()
