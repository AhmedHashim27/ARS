import socket
import struct
import time
import uasyncio
from machine import Pin
import network

import esp
esp.osdebug(None)

import gc
gc.collect()

import json

class Node():
    def __init__(self):
        self.host = "192.168.1.19"
        self.port = 30303
        self.sub_host = "192.168.4.1"
        self.sub_port = 50505
        self.server_host = "192.168.1.18"
        self.server_port = 30303
        self.timeout = 0.001

    def startWifi(self, ssid='MicroPython-AP', password='123456789'):
        ap = network.WLAN(network.AP_IF)
        ap.active(True)
        ap.config(essid=ssid, password=password)
        while ap.active() == False:
            pass

    def connectWifi(self):
        sta_if = network.WLAN(network.STA_IF);
        sta_if.active(True)
        sta_if.connect("TE-Data", "pwd3141592654")
        while not sta_if.isconnected():
            pass

    def startNetwork(self):
        self.startWifi()
        self.connectWifi()
        print('\nConnection successful')

    async def twistedRecv(self, sock):
        data = bytearray()
        while not data:
            try:
                data, addr = sock.recvfrom(1024)
            except Exception as e:
                await uasyncio.sleep(0)
                try:
                    self.reconnect(sock)
                except Exception as e:
                    pass
        try:
            data = json.loads(data.decode())
            if 'METHOD' in data:
                method = data["METHOD"]
                if method == "UPDATE_METHODS":
                    new_methods = data["METHODS"]
                    try:
                        exec(new_methods)
                        print("Methods updated successfully")
                    except Exception as e:
                        print("failed to update methods", e)
                elif method == "PUB_NODEMCU":
                    response = self.mainServerMethods(method, data["DATA"], data["TOPIC"], data["MESSAGE_ID"])
                    if response:
                        msg = str(json.dumps(response) + "\r\n").encode()
                        sock.sendto(msg, addr)

                elif method == "GET_INFO":
                    node_name, port_server, port_client = "nodemcu1", "30303", "50505"
                    data = {
                            "DEVICE": "nodemcu",
                            "METHOD": "INFO",
                            "NAME": node_name,
                            "TOPICS": ["led_data"],
                            "SERVER": port_server,
                            "CLIENT": port_client
                            }
                    msg = str(json.dumps(data) + "\r\n").encode()
                    sock.sendto(msg, addr)

            #print(data)
            sock.sendto(b'\r\n', addr)
        except Exception as e:
            print(e, "eee")
        #print("end-", time.ticks_ms())

    async def reconnect(self, sock):
        try:
            host_addr = (self.server_host, self.server_port)
            sock.connect(host_addr)
            sock.sendto(b'\r\n', host_addr)
            await uasyncio.sleep(0)
        except:
            pass

    def led(self, state):
        state = int(state)
        led = Pin(2, Pin.OUT)
        if state == 1:
            led.off()
        else:
            led.on()


    async def shareInfo(self, s):
        try:
            s.connect((self.server_host, self.server_port))
            node_name, port_server, port_client = "nodemcu1", self.port, "50505"
            info = {
                    "DEVICE": "nodemcu",
                    "METHOD": "INFO",
                    "NAME": node_name,
                    "TOPICS": ["led_data"],
                    "SERVER": port_server,
                    "CLIENT": port_client
                    }
            msg = str(json.dumps(info) + "\r\n").encode()
            s.sendto(msg, (self.server_host, self.server_port))
            while True:
                await self.twistedRecv(s)
                await uasyncio.sleep(0)
        except Exception as e:
            print(e)
            await uasyncio.sleep(0)

    async def listenServer(self, server):
        while True:
            try:
                data = bytearray()
                data, addr = server.recvfrom(256)
                data = json.loads(data.decode())
                method = data["METHOD"]
                response = json.dumps(self.localServerMethods(method, data["DATA"]))
                if response:
                    server.sendto(response.encode(), addr)
                else:
                    server.sendto(b'\r\n', addr)
            except Exception as e:
                await uasyncio.sleep(0)



    async def main(self):
        self.startNetwork()
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        s.bind((self.host, self.port))
        s.settimeout(self.timeout)
        server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        server.bind((self.sub_host, self.sub_port))
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.settimeout(self.timeout)
        await uasyncio.gather(self.listenServer(server), self.shareInfo(s))
