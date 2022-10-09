import re
import math
RAD_TO_DEG = 57.295

def filter_str(data):
	msg = data["DATA"]
	#msg = msg.replace("t", "f001")
	data["DATA"] = msg
	return data

def delete_numbers(data):
	msg = data["DATA"][0]
	a = msg
	Xangle =  (math.atan2(a[1],a[2])*RAD_TO_DEG)
	Yangle =  (math.atan2(a[2],a[0])*RAD_TO_DEG)
	Zangle = math.asin(a[2] / math.sqrt(a[0] ** 2 + a[1] **2 + a[2] ** 2)) * RAD_TO_DEG 

	data["DATA"] = [Xangle, Yangle, Zangle]
	return data
try:
	led = Pin(2, Pin.OUT)
	led.off()
except:
	pass