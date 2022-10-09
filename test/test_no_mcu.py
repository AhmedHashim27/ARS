from threading import Thread
from cluster import *
from uuid import uuid1
#import methods
import asyncio
import time
import time
from netifaces import interfaces, ifaddresses, AF_INET
for ifaceName in interfaces():
    addresses = [i['addr'] for i in ifaddresses(ifaceName).setdefault(AF_INET, [{'addr':'No IP addr'}] )]

features = {"NAME": "test", "TOPICS": [],"DESCRIPTION": "It's just at test", "METHODS": [], "CPU_USAGE": 0, "MEMORY_USAGE": 0, "ID": "1"}
node = Node()
node.node_name = "test"
node.cluster = addresses[0]
node.topics = {"test_topic1": {"METHODS": ["filter_str"]}}
node.description = "It's just at test"
node.methods = []
#c.subscribe("test_topic1", "test3")

#publish = Thread(target=c.Publish, args=("test_topic1", ["test6", "test5", "Main", "test3", "test"], str(uuid1())))

networks = ["WE_90C31E_EXT", "MicroPython-AP", "WE_90C31E", "afa"]
async def publ(test):
    try:

        i = 0
        while True:
            i += 1
            data = i % 2
            msg = await node.pub(data, "led_data", "nodemcu1", p_id, 50, ["filter_str", "delete_numbers"])
            print(msg)
    except Exception as e:
        exit()

p_id = node.Publish("led_data", ["Main", "test"], True)
node.run([publ])
#loop.close()
#c.get_details_per_client("test3")
