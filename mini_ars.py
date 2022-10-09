import socket
import json
import uasyncio

def send(data, sock):
    data = json.dumps(data).encode()
    recved = False
    sock.sendto(data, ("192.168.1.13", 50505))
    msg, addr = sock.recvfrom(1024)
    if msg:
    	print(msg)
    print('done')

def connect_wifi():
	sta_if = network.WLAN(network.STA_IF);
	sta_if.active(True)
	sta_if.connect("TE-Data", "pwd3141592654")
	while not sta_if.isconnected():
	    pass

	ip = sta_if.ifconfig()[0]
	print("connected -", ip)
	return ip

async def main():
	ip = connect_wifi()
	sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	sock.bind((ip, 30303))
	server.settimeout(0.1)
	while True:
		send({"METHOD": 'ENCODERS', "DATA": 5}, sock)
		await uasyncio.sleep(1)
		
uasyncio.run(main())