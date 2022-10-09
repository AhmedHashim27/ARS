from ars import *


ultrasonic = 0


def localServerMethods(self, method, data):
    global ultrasonic
    print(method, data)
    if method == "wait":
        time.sleep(int(data))
    elif method == "US":
        ultrasonic = data
    return {"METHOD": "RESPONSE", "US": ultrasonic}

def mainServerMethods(self, method, data, topic, msg_id):
    data = {
                "METHOD": "RESPONSE",
                "DATA": ultrasonic,
                "TOPIC": "ULTRASONIC_DATA",
                "MESSAGE_ID": msg_id
            }
    return data

Node.localServerMethods = localServerMethods
Node.mainServerMethods = mainServerMethods
node = Node()
uasyncio.run(node.main())