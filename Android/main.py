from threading import Thread
from cluster import *
from uuid import uuid1
#import methods
import asyncio
import time
import androidhelper

node = Node()
node.node_name = "android"
node.cluster = input("master ip: ")
#node.topics = {"test_topic1": {"METHODS": ["filter_str"]}}
node.description = "It's just at test"
node.methods = []
#c.subscribe("test_topic1", "test3")

#publish = Thread(target=c.Publish, args=("test_topic1", ["test6", "test5", "Main", "test3", "test"], str(uuid1())))
data = {}
droid = androidhelper.Android()
async def publ(test):
    try:
        droid.startSensingTimed(1, 50)
        while True:
            try:
                rssi = droid.wifiGetConnectionInfo()
                sensor = droid.sensorsReadAccelerometer().result
                data = {"RSSI": rssi, "ACC": sensor}
            except:
                pass
            msg = await node.pub(data, "localization", "pose", p_id, 20, [])
            print(msg)
    except Exception as e:
        droid.stopSensing()
        exit()

p_id = node.Publish("localization", ["Main", "pose"], True)
node.run([publ])
#loop.close()
#c.get_details_per_client("test3")