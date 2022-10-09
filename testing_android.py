from threading import Thread
from cluster import *
from uuid import uuid1
#import methods
import asyncio
import time

node = Node()
node.node_name = "android"
node.cluster = "192.168.1.28"
node.topics = {"test_topic1": {"METHODS": ["filter_str"]}}
node.description = "It's just at test"
node.methods = []
#c.subscribe("test_topic1", "test3")

#publish = Thread(target=c.Publish, args=("test_topic1", ["test6", "test5", "Main", "test3", "test"], str(uuid1())))
data = {}
async def publ(test):
    try:
        while True:
            try:
                rssi = 1
                sensor = 2
                data = {"RSSI": rssi, "ACC": sensor}
            except:
                pass
            msg = await node.pub(data, "localization", "pose", p_id, 20, [])
            print(msg)
    except Exception as e:
        exit()

p_id = node.Publish("localization", ["Main", "pose"])
node.run([publ])
#loop.close()
#c.get_details_per_client("test3")
