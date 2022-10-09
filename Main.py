from cluster import *
import methods

node = Node()
node.cluster = "192.168.1.28"
node.port_server = "10101"
node.topics = {}
node.description = "It's just at test"
node.methods = []
node.run()