import paho.mqtt.client as mqtt
import time, sys
import yaml
import json
import random
import pprint

# pp = pprint.PrettyPrinter(indent=2) #print pretty json
# yaml.warnings({'YAMLLoadWarning': False})


def my_print(d):
	try:
		level = int(cfg["debug"])
	except:
		print("don't have or wrong type 'debug' option, please check config.yaml!")
		exit(-1)
	if level == 1:
		print(d)


uid = random.randint(1, 1000000)
# Load config.yaml
try:
	with open("config.yaml", "r") as f:
		cfg = yaml.load(f)
except:
	print("don't find 'config.yaml'")
	exit(-1)


# Create List symbols
l_symbols = dict()
# Open log file
try:
	result_file = open(cfg["logFileName"], "w+")
except:
	print("don't have 'logFileName' option, please check config.yaml!")
	exit(-1)


def datachanged_on_message(client, userdata, message):
	msg = str(message.payload.decode("utf-8"))
	my_print(msg)
	# parse json
	msg_json = json.loads(msg)
	for key in msg_json["symbols"].keys():
		l_symbols[key] = msg
	# Write to file
	result_file.seek(0)
	for key in l_symbols.keys():
		result_file.write(l_symbols[key] + "\r\n")


def on_connect(client, userdata, flags, rc):
	sw_connection_result = {
		0: "Connection successful",
		1: "Connection refused - incorrect protocol version",
		2: "Connection refused - invalid client identifier",
		3: "Connection refused - server unavailable",
		4: "Connection refused - bad username or password",
		5: "Connection refused - not authorised"
	}

	print(sw_connection_result.get(rc, "Connection refused - Other failed"))
	if rc == 0:
		client.subscribe(cfg["dataInfo"]["datachanged_bind_Key"], int(cfg["dataInfo"]["datachanged_qos"]))
		print("Subcribe '{}' Success".format(cfg["dataInfo"]["datachanged_bind_Key"]))


def on_disconnect(client, userdata, rc):
	if rc != 0:
		print("Unexpected MQTT disconnection. Will auto-reconnect")


def main():
	# Create a mqtt client object
	try:
		c_datachanged = mqtt.Client(cfg["dataInfo"]["datachanged_queue_name"] + "_" + str(uid))
	except:
		print("Initial mqtt Client failed, Please check 'dataInfo' & 'datachanged_queue_name' options!")
		exit(-1)
	# Set username & password
	try:
		c_datachanged.username_pw_set(cfg["serverInfo"]["username"], cfg["serverInfo"]["password"])
	except:
		print("Set Username & Password failed, Please check 'serverInfo' & 'username' & 'password' options!")
		exit(-1)
	# Set the mqtt client other options
	c_datachanged.on_message = datachanged_on_message
	c_datachanged.on_disconnect = on_disconnect
	c_datachanged.on_connect = on_connect
	# Connecto to Server
	try:
		c_datachanged.connect(cfg["serverInfo"]["host"], int(cfg["serverInfo"]["port"]))
	except:
		print("Connect to server failed, Please check 'host' & 'port' options!")
		exit(-1)

	#c_datachanged.loop_start()
	c_datachanged.loop_forever()

	while 1:
		# print("%%%%%%%%%%%%%%%\n")
		# pp.pprint(l_symbols)
		# print("\n%%%%%%%%%%%%%%%\n")
		# datafirst_log.write(msg)
		time.sleep(0.1)

	print("Closed!")
	result_file.close()


if __name__ == "__main__":
	main()
