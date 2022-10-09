import socket
import json
import uasyncio
import network

def send(data, sock):
    data = json.dumps(data).encode()
    recved = False
    sock.sendto(data, ("192.168.4.1", 50505))
    msg, addr = sock.recvfrom(1024)
    if msg:
    	print(msg)
    print('done')

def connect_wifi():
	ap_if = network.WLAN(network.AP_IF)
	ap_if.active(False)
	sta_if = network.WLAN(network.STA_IF);
	sta_if.active(True)
	sta_if.connect("MicroPython-AP", "123456789")
	while not sta_if.isconnected():
	    pass

	ip = sta_if.ifconfig()[0]
	print("connected -", ip)
	return ip

async def main():
	ip = connect_wifi()
	server = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
	server.bind((ip, 30303))
	server.settimeout(1)
	print("Start")
	while True:
		print("Sent")
		try:
			send({"METHOD": 'US', "DATA": -8}, server)
			await uasyncio.sleep(.1)
		except Exception as e:
			print(e)
		
uasyncio.run(main())