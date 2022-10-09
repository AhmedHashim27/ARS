from cluster import *

features = {"NAME": "test5", "TOPICS": [],"DESCRIPTION": "It's just at test", "METHODS": [], "CPU_USAGE": 0, "MEMORY_USAGE": 0, "ID": "1"}
node = Node()
node.node_name = "test5"
node.cluster = "192.168.1.4"
node.topics = {}
node.description = "It's just at test"
node.methods = []
'''
pub = Thread(target=c.Publish, args=("test_topic1", "test3"))
pub.daemon = True
pub.start()
'''
node.run()