from threading import Thread
import sys
sys.path.append('/home/manwar/work/FRS/')
from cluster import *
import methods
#import bpy
import json

#features = {"NAME": "test3", "TOPICS": {"test_topic1": {"METHODS": ["filter_str"]}},"DESCRIPTION": "It's just at test", "METHODS": [], "CPU_USAGE": 0, "MEMORY_USAGE": 0, "ID": "1"}
node = Node()
node.node_name = "test3"
node.cluster = "192.168.1.4"
node.topics = {"test_topic1": {"METHODS": ["filter_str"]}}
node.description = "It's just at test"
node.methods = []

def callback(data):
    bpy.data.objects["Cube"].rotation_euler = data["DATA"]
    #print(data["MESSAGE_ID"][-3:])
    print(data["DATA"])

#pub = Thread(target=c.run, args=([], {"test_topic1": callback}))
#pub.daemon = True
#pub.start()
node.run([], {"test_topic1": callback})