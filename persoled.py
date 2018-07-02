#!/usr/bin/python3

import sys
import json
import pickle

from kafka import KafkaConsumer,TopicPartition
from scipy.spatial import distance


import conf as co



def comp(d1, d2):
    if distance.euclidian(d1,d2) > 0.7:
        return False
    else:
        return True


line = sys.stdin.readline().strip()

topics = co.ka_topic
partition = co.ka_partition

consumer = KafkaConsumer(bootstrap_servers=co.ka_host, auto_offset_reset='earliest')
tp = TopicPartition(topics,partition)
consumer.assign([tp])
consumer.seek_to_end(tp)
lastOffset = consumer.position(tp)
consumer.seek_to_beginning(tp)

if lastOffset == 0:
    print("EVT_VISITOR_DETECTED:None\n")
    sys.exit()


if line == "":
    print("error")
    sys.exit()

else:
    d = json.loads(line)
    for m in consumer:
        if m.offset == lastOffset - 1:
            print("EVT_VISITOR_DETECTED:None\n")
            sys.exit()
        else: 
            if comp(d["descriptor"],pickle.loads(m)["descriptor"]):
                print("EVT_PERSON_DETECTED:%s\n" % pickle.loads(m)["id"])
                sys.exit()
                