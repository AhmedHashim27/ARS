from twisted.internet import reactor, protocol, endpoints, threads
from twisted.protocols import basic
from random_open_port import random_port
import telnetlib
import json
import sys
import os
import time
import asyncio
from threading import Thread
import socket
from pympler.asizeof import asizeof
#import methods
#import importlib
from uuid import uuid1
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from pathlib import Path
import struct
from twisted.internet.protocol import DatagramProtocol
from twisted.internet import reactor


class Node():
    def __init__(self):
        self.node_name = "Main"
        self.id = str(uuid1())

        self.cluster = "127.0.0.1"
        self.port_server = "10101"
        self.port_client = "20202"
        
        self.tree = {}
        self.features = {}
        self.methods = {}
        self.memory_usage = 0
        self.description = None
        
        self.tasks = []
        self.timeout = 3
        self.callback = None
        self.encoding = "UTF8"
        self.current_server = "Main"
        self.servers_stack = ["Main"] # Servers Stack is used for checking nearest cluster to communicate directly
        
        self.node_status = {}
        self.enabled_cache = {}
        self.max_cache = 1048576
        self.cache_max_reached = False

        self.publishers = {}
        self.publishers_cache = {}
        self.nodes_connection = {}
        self.publishers_connection = {}

        self.watching = False
        self.clients_info = {}
        self.nodes_details = {}
        self.clients_addresses = {}
        self.cluster_listener = None
        self.nodemcus = {}
        self.nodemcu_msgs = {}
        self.nodemcu_response = {}
        self.nodemcu_response_msgs = {}
        self.nodemcus_topics = {}
        self.nodemcus_check = {}
        self.nodemcus_comm = {}
        self.nodemcus_connections = {}

    # Identity Server
    class IdentityProtocol(basic.LineReceiver):
        def __init__(self, factory):
            self.factory = factory
            self.cluster = factory.cluster
            if self.cluster.node_name == "Main" and not self.cluster.watching:
                self.watchMethods()
                self.cluster.watching = True

        def connectionMade(self):
            print('made')
            self.transport.setTcpKeepAlive(True)
            self.factory.clients.add(self)
            self.shareInfo()
            self.shareClients()
            if self.cluster.node_name == "Main":
                self.updateMethods("", self)

        def connectionLost(self, reason):
            peer = self.transport.getPeer()
            peer_url = self.cluster.getPeerUrl(peer)
            if peer_url in self.cluster.clients_addresses and self.cluster.clients_addresses[peer_url] in self.cluster.clients_info:
                self.removeClient(self.cluster.clients_addresses[peer_url])
                self.cluster.clients_info.pop(self.cluster.clients_addresses[peer_url])
                self.cluster.clients_addresses.pop(peer_url)
            try:
                self.factory.clients.remove(self)
            except:
                pass
            self.cluster.updateConnection(self.cluster.clients_info)
            self.shareClients()

        def lineReceived(self, line):
            if line:
                print(line)
                data = self.cluster.loadJsonData(line)
                #self.cluster.sendJsonData(self, data)
                peer = self.transport.getPeer()
                METHOD = data["METHOD"]

                if METHOD == "CHECK_STATUS":
                    features = self.cluster.features
                    self.cluster.telnetCacheLaunch()
                    cache_size = asizeof(self.cluster.publishers_cache)
                    features["MEMORY_USAGE"] = int((cache_size / self.cluster.max_cache) * 100)
                    if self.cluster.cache_max_reached:
                        features["MEMORY_USAGE"] = 100
                    data = {"METHOD": "STATUS", "PUBLISHER_ID": data["PUBLISHER_ID"], "STATUS": features}
                    self.cluster.sendJsonData(self, data)

                elif METHOD == "STATUS":
                    publisher_id = data["PUBLISHER_ID"]
                    status = data["STATUS"]
                    #self.cluster.node_status[publisher_id] = (int(status["MEMORY_USAGE"]) < 100 and int(status["CPU_USAGE"]) < 100)
                    self.cluster.node_status[publisher_id] = (int(status["MEMORY_USAGE"]) < 100)
                    self.shareClients()

                elif METHOD == "UPDATE_METHODS":
                    try:
                        print("Methods have been installed")
                        exec(data["METHODS"])
                    except Exception as e:
                        print("New methods installation error - {}".format(e))

                elif METHOD == "INFO":
                    data["HOST"] = peer.host
                    url = self.cluster.getPeerUrl(peer)
                    self.cluster.clients_info[data["NAME"]] = data
                    self.cluster.clients_addresses[url] = data["NAME"]
                    self.cluster.nodes_connection[data["NAME"]] = self
                    for server in self.cluster.tree:
                        if data["NAME"] in self.cluster.tree[server] and server != self.cluster.node_name:
                            redirect = {"METHOD": "REDIRECT", "SERVER": server}
                            self.cluster.sendJsonData(self, redirect)
                    self.shareClients()

                elif METHOD == "CLIENTS":
                    clients = data["CLIENTS"]
                    clients_changed = False
                    if self.cluster.node_name not in clients:
                        self.shareInfo()
                    for client in clients:
                        is_new_client = not ((client in self.cluster.clients_info) and (self.cluster.clients_info[client] == clients[client]))
                        clients_changed = (clients_changed or (is_new_client))
                        if self.cluster.node_name in clients and client == self.cluster.node_name :
                            client_port = clients[client]["CLIENT"]
                            server_port = clients[client]["SERVER"]
                            #print(client_port != self.cluster.port_client or server_port != self.cluster.port_server)
                            if client_port != self.cluster.port_client or server_port != self.cluster.port_server:
                                self.shareInfo()
                                #print("Solved old caches")

                        else:
                            self.cluster.clients_info[client] = clients[client]

                    if clients_changed:
                        self.cluster.updateConnection(clients)

                elif METHOD == "REDIRECT":
                    server = data["SERVER"]
                    if server != self.cluster.current_server and server not in self.cluster.servers_stack:
                        self.cluster.servers_stack.insert(0, server)

                elif METHOD == "REOMVE_CLIENT":
                    for publisher in self.cluster.publishers:
                        if data["CLIENT"] in self.cluster.publishers[publisher]:
                            self.cluster.publishers_connection[publisher].close()
                    if data["CLIENT"] in self.cluster.clients_info:
                        self.cluster.clients_info.pop(data["CLIENT"])
                    else:
                        print('couldn\'t remove')

                else:
                    print(data)

        def removeClient(self, name):
            for c in self.factory.clients:
                self.cluster.sendJsonData(c, {"METHOD": "REOMVE_CLIENT", "CLIENT": name})

        def shareClients(self):
            for c in self.factory.clients:
                self.cluster.sendJsonData(c, {"METHOD": "CLIENTS", "CLIENTS": self.cluster.clients_info})
        
        def shareInfo(self):
            info = {
                        "METHOD": "INFO",
                        "NAME": self.cluster.node_name,
                        "SERVER": self.cluster.port_server,
                        "CLIENT": self.cluster.port_client
                    }
            for c in self.factory.clients:
                self.cluster.sendJsonData(c, info)
    
        def watchMethods(self, path="methods.py"):
            class Handler(FileSystemEventHandler):
                def __init__(self, protocol):
                    self.protocol = protocol

                def on_modified(self, event):
                    if event.src_path == "./" + path: # in this example, we only care about this one file
                        print("Methods changed")
                        new_methods = Path("./" + path).read_text()
                        self.protocol.updateMethods(new_methods)

            observer = Observer()
            observer.schedule(Handler(self), ".") # watch the local directory
            observer.start()

        def updateMethods(self, new_methods="", client=None):
            if not new_methods:
                new_methods = Path("./methods.py").read_text()
            data = {
                        "METHOD": "UPDATE_METHODS",
                        "METHODS": new_methods,
                        "MESSAGE_ID": str(uuid1())
                    }
            if not client:
                for c in self.factory.clients:
                    self.cluster.sendJsonData(c, data)
                return
            self.cluster.sendJsonData(client, data)

    class IdentityFactory(protocol.ReconnectingClientFactory):
        maxDelay = 3.0 # Max Delay to reconnect
        def __init__(self, cluster):
            self.clients = set()
            self.cluster = cluster
            self.IdentityProtocol = cluster.IdentityProtocol

        def clientConnectionFailed(self, connector, reason):
            self.reconnect(connector, reason)

        def clientConnectionLost(self, connector, reason):
            self.reconnect(connector, reason)

        def reconnect(self, connector, reason):
            try:
                peer = connector.transport.getPeer()
                peer_url = self.cluster.getPeerUrl(peer)
                if url in self.cluster.clients_addresses:
                    name = self.cluster.clients_addresses[peer_url]
                    if name == self.cluster.current_server:
                        del self.cluster.servers_stack[name]
                        self.cluster.current_server = "Main"
                        self.cluster.updateConnection()

                    self.cluster.clients_info.pop(self.cluster.clients_addresses[peer_url])
                    self.cluster.clients_addresses.pop(url)

                if connector in self.clients:
                    self.clients.remove(connector)
            except:
                pass
            protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

        def buildProtocol(self, addr):
            self.resetDelay()
            return self.IdentityProtocol(self)

    # Communication Server
    class CommunicateProtocol(basic.LineReceiver):
        def __init__(self, factory):
            self.factory = factory
            self.cluster = factory.cluster

        def connectionMade(self):
            self.transport.setTcpKeepAlive(True)
            self.factory.clients.add(self)
            features = {"METHOD": "DETAILS"}
            features["FEATURES"] = self.cluster.features
            self.cluster.sendJsonData(self, features)

        def lineReceived(self, line):
            if len(line):
                data = json.loads(line.decode(self.cluster.encoding))
                METHOD = data["METHOD"]
                if METHOD == "PUB":
                    topic = data["TOPIC"]
                    sub_node = data["SUBSCRIBER"]
                    publisher_id = data["PUBLISHER_ID"]
                    #self.cluster.nodeStatus(publisher_id)
                    if sub_node == self.cluster.node_name:
                        try:
                            self.cluster.callback[topic](data)
                        except Exception as e:
                            print("Cannot handle received data", e, data["DATA"])
                    else:
                        for method in data["METHODS"]:
                            if method in self.cluster.methods:
                                #print('method', method)
                                data = self.cluster.topicThroughProccessing(data, method)
                                #print("Proccessing data", method)
                        if publisher_id not in self.cluster.publishers_connection:
                            self.cluster.telnetAddToCache(data)
                            print("Connection hasn't established yet")
                        elif publisher_id in self.cluster.node_status and not self.cluster.node_status[publisher_id]:
                            self.cluster.telnetAddToCache(data)
                            print("Next client is full. I will cache it until it's available")
                        else:
                            self.cluster.telnetAddToCache(data)
                            #print("Data passed to {} - {}".format(topic, sub_node))
                elif METHOD == "PUB_NODEMCU":
                    try:
                        node = data["SUBSCRIBER"]
                        if node in self.cluster.nodemcus_comm:
                            self.cluster.nodemcus_comm[node](data, self.cluster.nodemcus[node], self)
                            print("sent")
                        else:
                            self.transport.loseConnection()
                    except Exception as e:
                        print(e)

                elif data["METHOD"] == "DETAILS":
                    features = data["FEATURES"]
                    name = features["NAME"]
                    self.cluster.nodes_details[name] = features

                elif data["METHOD"] == "THROUGH":
                    print(self.cluster.nodemcus_topics)
                    if data["TOPIC"] in self.cluster.nodemcus_topics:
                        address = self.cluster.nodemcus[nodes[0]]
                        if address in self.cluster.nodemcu_msgs:
                            self.cluster.nodemcu_msgs[address].append(data)

                    #if data["PUBLISHER_ID"] not in self.cluster.publishers:
                    self.cluster.Publish(data["TOPIC"], data["NODES"], False, data["PUBLISHER_ID"])
                    print("sent through", data["TOPIC"], data["NODES"])
                elif METHOD == "RESPONSE":
                    print("res", data)
        def shareDetails(self):
            for c in self.factory.clients:
                self.cluster.sendJsonData(c, {"METHOD": "CLIENTS", "CLIENTS": self.cluster.clients_info})
    
    class CommunicateFactory(protocol.ReconnectingClientFactory):
        maxDelay = 3.0 # Max Delay to reconnect
        def __init__(self, cluster):
            self.clients = set()
            self.cluster = cluster
            self.CommunicateProtocol = cluster.CommunicateProtocol

        def clientConnectionFailed(self, connector, reason):
            self.reconnect(connector, reason)

        def clientConnectionLost(self, connector, reason):
            self.reconnect(connector, reason)

        def reconnect(self, connector, reason):
            protocol.ReconnectingClientFactory.clientConnectionLost(self, connector, reason)

        def buildProtocol(self, addr):
            self.resetDelay()
            return self.CommunicateProtocol(self)


    class NodeMCUClient(DatagramProtocol):
        def __init__(self, cluster, host='192.168.1.28', port=30303):
            self.cluster = cluster
            self.host = host
            self.port = port

        def startProtocol(self):
            #self.transport.write(b"C", (self.host, self.port))
            print("new")

        def datagramReceived(self, data, address):
            #print(data, address)
            if data == b'\r\n':
                if address not in self.cluster.nodemcus_connections:
                    self.pub({"METHOD": "GET_INFO", "MESSAGE_ID": -1}, address, (self.host, self.port))
                '''
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(self.pub(address))
                '''
            elif data:
                data = json.loads(data.decode())
                if "DEVICE" in data:
                    if data["DEVICE"] == "nodemcu":
                        self.cluster.nodemcus[data["NAME"]] = address
                        self.cluster.nodemcus_comm[data["NAME"]] = self.pub
                        self.cluster.nodemcus_topics[data["NAME"]] = data["TOPICS"]
                        self.cluster.nodemcus_connections[address] = data["NAME"]
                        print(self.cluster.nodemcus_comm)
                        loop = asyncio.new_event_loop()
                        asyncio.set_event_loop(loop)
                        loop.run_until_complete(self.sendMethods(data["NAME"]))
                elif "METHOD" in data:
                    method = data["METHOD"]
                    if method == "PUB":
                        print(data["DATA"])
                    elif method == "RESPONSE":
                        try:
                            print("pub", data)
                            loop = asyncio.new_event_loop()
                            asyncio.set_event_loop(loop)
                            loop.run_until_complete(self.responseJson(data, self.cluster.nodemcu_response[address][data["MESSAGE_ID"]]))
                            del self.cluster.nodemcu_response[address][data["MESSAGE_ID"]]
                        except Exception as e:
                            print(e)
            '''
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
                sock.connect((ip, port))
                print('Sending to server...')
                send({"METHOD": "test"}, sock)
                send({"METHOD": "exit1"}, sock)
                send({"METHOD": "exit2"}, sock)
                send({"METHOD": "exit3"}, sock)
                send({"METHOD": "exit"}, sock)
            print('stopped')
            '''

        async def sendMethods(self, peer):
            methods = Path("./methods.py").read_text()
            data = {
                    "METHOD": "UPDATE_METHODS",
                    "METHODS": methods,
                    "MESSAGE_ID": str(uuid1())
                }
            addr = self.cluster.nodemcus[peer]
            await self.writeJson(data, addr)

        def pub(self, msg, addr, frm):
            if addr in self.cluster.nodemcu_response:
                if len(self.cluster.nodemcu_response[addr]) > 100:
                    self.cluster.nodemcu_response[addr].popitem()
                self.cluster.nodemcu_response[addr][msg["MESSAGE_ID"]] = frm
            else:
                self.cluster.nodemcu_response[addr] = {msg["MESSAGE_ID"]: frm}
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self.writeJson(msg, addr))

        async def writeJson(self, data, addr):
            data = str(json.dumps(data) + "\r\n")
            #self.transport.write(str(json.dumps({"LENGTH": len(data)})).encode(), addr)
            await asyncio.sleep(0.001)
            self.transport.write(data.encode(), addr)

        async def responseJson(self, data, peer):
            #data = str(json.dumps(data) + "\r\n")
            #await asyncio.sleep(0.001)
            peer.sendLine(json.dumps(data).encode())

    def sendnodemcu(data, sock):
        msg = json.dumps(data).encode()
        msg = struct.pack('>I', len(msg)) + msg
        sock.sendall(msg)

    # Start Server
    def StartServer(self, port_server, port_client, tasks, callback):
        print(port_server, port_client)
        endpoints.serverFromString(reactor, "tcp:" + str(port_server)).listen(self.IdentityFactory(self))
        endpoints.serverFromString(reactor, "tcp:" + str(port_client)).listen(self.CommunicateFactory(self))
        if self.node_name == "Main":
            reactor.listenMulticast(30303, self.NodeMCUClient(self), listenMultiple=True)
        self.tasks = tasks
        self.callback = callback
        if tasks:
            tasks = threads.deferToThread(self.startTasks)
            self.running_tasks = tasks
            reactor.addSystemEventTrigger('before', 'shutdown', self.close)
        reactor.run()

    def close(self):
        self.running_tasks.cancel()
        print("bye")
        os._exit(1)

    # Connect to the nearest cluster
    def updateConnection(self, data):
        for i in self.servers_stack:
            if len(data) > 0 and i in data and i != "Main":
                server = data[i]
                cluster_ip = server["HOST"]
                cluster_port = int(server["SERVER"])
                if hasattr(self.cluster_listener, 'state'):
                    if self.cluster_listener.state != "connected":
                        self.cluster_listener.disconnect()

                new_connection = reactor.connectTCP(cluster_ip, cluster_port, self.IdentityFactory(self))
                self.cluster_listener = new_connection
                #if self.current_server != i: print("Connected to the new cluster", i, self.current_server)
                self.current_server = i
                break

    def nodeStatus(self, publisher_id): # check node status
        connections = self.nodes_connection
        for conn in connections:
            conn = connections[conn]
            self.getNodeStatus(publisher_id, conn)


    def getNodeStatus(self, publisher_id, conn):
        self.sendJsonData(conn, {"METHOD": "CHECK_STATUS", "PUBLISHER_ID": publisher_id})


    # Get info about clients after connectoins are done
    def getClientDetails(self, client_name):
        try:
            client = self.clients_info[client_name]
            print(client, "-"*10)
            host, port = client["HOST"], client["CLIENT"]
            print(host, port)
            tn = telnetlib.Telnet(host, port)
            return tn, host, port
        except Exception as e:
            print(e)
            return

    async def initPublish(self, topic, nodes, publisher_id, nodemcu=False):
        print("initPublish")
        if nodemcu:
            if len(nodes) > 1:
                node = list(nodes)[0]
                try:
                    client_conn, host, client_port = self.getClientDetails(node)
                    self.publishers_connection[publisher_id] = client_conn # Just to resend cache
                    self.publishers[publisher_id] = nodes
                    print(host, client_port, self.clients_info)
                except Exception as e:
                    print(e)
                    await asyncio.sleep(self.timeout)
                    await self.initPublish(topic, nodes, publisher_id, nodemcu)
                    return

                print("node connected {}".format(node))
                #client_id = id(client_conn)
                try:
                    data = client_conn.read_until(b"\r\n")
                    client_conn.write(b"\r\n")
                except Exception as e:
                    print(e)
                    await self.initPublish(topic, nodes, publisher_id, nodemcu)
                    return

                details = self.getJsonData(data)
                features = details["FEATURES"]
                name = features["NAME"]
                self.nodes_details[name] = features

                while node not in self.nodes_details:
                    print("Wait to initiate a connection with", node)
                    await asyncio.sleep(self.timeout)

                try:
                    while True:
                        try:
                            client_conn.write(b"\r\n")
                            data = client_conn.read_until(b"\r\n", timeout=1)
                            data = json.loads(data.decode())
                            method = data["METHOD"]
                            if method == "RESPONSE":
                                self.nodemcu_response_msgs[data["MESSAGE_ID"]] = data
                        except socket.timeout:
                            pass
                        except Exception as e:
                            break

                except:
                    print("UNCONNECTED")

                if len(list(nodes)) > 0 and list(nodes)[0] in self.clients_info:
                    del self.clients_info[list(nodes)[0]]

                await asyncio.sleep(self.timeout)
                await self.initPublish(topic, nodes, publisher_id, nodemcu)
        else:
            if len(nodes) > 0:
                node = list(nodes)[0]
                try:
                    client_conn, host, client_port = self.getClientDetails(node)
                    self.publishers_connection[publisher_id] = client_conn # Just to resend cache
                    self.publishers[publisher_id] = nodes
                    print(host, client_port, self.clients_info)
                except Exception as e:
                    print(e)
                    await asyncio.sleep(self.timeout)
                    await self.initPublish(topic, nodes, publisher_id)
                    return

                print("node connected {}".format(node))
                #client_id = id(client_conn)
                try:
                    data = client_conn.read_until(b"\r\n")
                    client_conn.write(b"\r\n")
                except Exception as e:
                    print(e)
                    await self.initPublish(topic, nodes, publisher_id)
                    return

                details = self.getJsonData(data)
                features = details["FEATURES"]
                name = features["NAME"]
                self.nodes_details[name] = features

                while node not in self.nodes_details:
                    print("Wait to initiate a connection with", node)
                    await asyncio.sleep(self.timeout)

                #print(topic, self.nodes_details[node]["TOPICS"], node)
                if topic in self.nodes_details[node]["TOPICS"]:
                    print("SUCCESSFULL CONNECTION ( with {} node - {} topic )".format(node, topic))
                else:
                    pub = {"METHOD": "THROUGH", "TOPIC": topic, "NODES": nodes[1:], "PUBLISHER_ID": publisher_id}
                    self.telnetWriteJson(client_conn, pub)
                    print("{} node doesn't have {} topic".format(node, topic))

                try:
                    while True:
                        try:
                            client_conn.write(b"\r\n")
                            x = client_conn.read_until(b"\r\n", timeout=1)
                        except socket.timeout:
                            pass
                        except Exception as e:
                            break

                except:
                    print("UNCONNECTED")

                if len(list(nodes)) > 0 and list(nodes)[0] in self.clients_info:
                    del self.clients_info[list(nodes)[0]]

                await asyncio.sleep(self.timeout)
                await self.initPublish(topic, nodes, publisher_id)

    def telnetWriteJson(self, client, data):
        try:
            data_str = json.dumps(data) + "\r\n"
            client.write(b"\r\n")
            client.write(data_str.encode(self.encoding))
            print('data has sent', data_str)
        except Exception as e:
            print("Data went into cache")
            return self.telnetAddToCache(data)

    def telnetWriteCachedData(self, publisher_id, retrying=False):
        if not retrying:
            if publisher_id in self.enabled_cache and self.enabled_cache[publisher_id] == 1:
                #print("cache already enabled")
                return -1 # Cache is enabled

        resolved_cache = []
        self.enabled_cache[publisher_id] = 1
        for message_id in self.publishers_cache[publisher_id]:
            if not (publisher_id in self.node_status and not self.node_status[publisher_id]):
                message = self.publishers_cache[publisher_id][message_id]
                try:
                    #print("trying to connect to server ( cached data )")
                    client = self.publishers_connection[message["PUBLISHER_ID"]]
                    message_str = json.dumps(message) + "\r\n"
                    client.write(b"\r\n")
                    client.write(message_str.encode(self.encoding))
                    resolved_cache.append(message_id)
                except Exception as e:
                    pass
            
        for message in resolved_cache:
            del self.publishers_cache[publisher_id][message]
        if len(self.publishers_cache[publisher_id]) == 0:
            del self.publishers_cache[publisher_id]
        self.enabled_cache[publisher_id] = 0

    def telnetAddToCache(self, data):
        if "MESSAGE_ID" in data: # check if data is a message to handle it
            message_id = data["MESSAGE_ID"]
            publisher_id = data["PUBLISHER_ID"]
            cache_size = asizeof(self.publishers_cache)
            if cache_size <= self.max_cache:
                self.cache_max_reached = False
                if publisher_id not in self.publishers_cache:
                    self.publishers_cache[publisher_id] = {message_id: data}
                if message_id not in self.publishers_cache[publisher_id]:
                    self.publishers_cache[publisher_id][message_id] = data
                #print(cache_size, self.max_cache, len(self.publishers_cache[publisher_id]), message_id, "has been added")
                self.telnetCacheLaunch(publisher_id)
            else:
                self.cache_max_reached = True
                print("Cache is full")
                return -2 # Cache is full

    def telnetCacheLaunch(self, publisher_id=None):
        if len(self.publishers_cache) > 0:
            #print("Launching Cache")
            if not publisher_id:
                for publisher_id in list(self.publishers_cache):
                    self.telnetWriteCachedData(publisher_id)
            else:
                self.telnetWriteCachedData(publisher_id)

    def Publish(self, topic, node, nodemcu=False, id=str(uuid1())):
        publish = Thread(target=self.newThreadPublisher, args=(topic, node, id, nodemcu))
        publish.daemon = True
        publish.start()
        return id

    def newThreadPublisher(self, topic, node, id, nodemcu):
        print("Start Subscribing", node)
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(self.initPublish(topic, node, id, nodemcu))
        loop.run_forever()

    def multiPublish(self, topic, nodes, id=str(uuid1())):
        publish = Thread(target=self.newThreadPublisher, args=(topic, node, id))
        publish.daemon = True
        publish.start()

    async def pub(self, data, topic, subscriber, publisher_id="5476ca6c-e06b-11ec-aa3a-8ec76bd63f57", rate=10, methods=[], nodemcu=False, response=False):
        try:
            msg_id = str(uuid1())
            print(data)
            if nodemcu:
                self.telnetWriteJson(self.publishers_connection[publisher_id], {"METHOD": "PUB_NODEMCU", "TOPIC": topic, "SUBSCRIBER": subscriber, "DATA": data, "MESSAGE_ID": msg_id})
                if response:
                    timeout = 0
                    while msg_id not in self.nodemcu_response_msgs and timeout < 10*(1/rate):
                        await asyncio.sleep(1/rate)
                        timeout += 1/rate
            else:
                self.telnetAddToCache({"METHOD": "PUB", "METHODS": methods, "TOPIC": topic, "SUBSCRIBER": subscriber, "DATA": data, "PUBLISHER_ID": publisher_id, "MESSAGE_ID": msg_id})
        except Exception as e:
            await asyncio.sleep(3)
        if msg_id in self.nodemcu_response_msgs:
            msg = self.nodemcu_response_msgs[msg_id]
            del self.nodemcu_response_msgs[msg_id]
            return msg
        else:
            await asyncio.sleep(1/rate)
        return msg_id

    def startTasks(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.create_task(self.multipleTasks())
        loop.run_forever()

    async def multipleTasks(self):
        input_coroutines = [x('test') for x in self.tasks]
        res = await asyncio.gather(*input_coroutines, return_exceptions=True)

    def loadJsonData(self, data):
        return json.loads(data.decode(self.encoding))

    def sendJsonData(self, peer, data):
        peer.sendLine(json.dumps(data).encode(self.encoding))

    def sendNodeMCUJsonData(self, peer, data):
        msg = json.dumps(data).encode()
        print(len(msg))
        peer.sendLine(str(json.dumps({"LENGTH": len(msg)}) + "\r\n").encode())
        peer.sendLine(msg)
        '''
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.connect(("192.168.1.13", 50000))
            print('Sending to server...')
            sock.sendall(msg)
        '''

    def getPeerUrl(self, peer):
        return peer.host + ":" + str(peer.port)

    def getJsonData(self, data):
        return json.loads(data.decode(self.encoding))

    def topicThroughProccessing(self, data, method):
        return getattr(methods, method)(data)

    def initFeatures(self):
        self.features = {"NAME": self.node_name, "TOPICS": self.topics,"DESCRIPTION": self.description, "METHODS": self.methods, "MEMORY_USAGE": self.memory_usage, "ID": self.id}

    def run(self, tasks=[], callback=[]):
        # Check if cluster already running
        try:
            self.initFeatures()
            tn = telnetlib.Telnet(self.cluster, self.port_server, timeout=1)
            tn.close()
            self.cluster_listener = reactor.connectTCP(self.cluster, int(self.port_server), self.IdentityFactory(self))
            print("connected to the Main-Cluster")
            self.port_server, self.port_client = str(random_port()), str(random_port())
        except Exception as e: # The cluster hasn't initialized yet ( Initiate A Cluster )
            print("Cluster has started", e)
        self.StartServer(self.port_server, self.port_client, tasks, callback)
