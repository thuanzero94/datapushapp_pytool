import paho.mqtt.client as mqtt
import time, sys
import yaml
import json
import random
import pprint

pp = pprint.PrettyPrinter(indent=2) #print pretty json
# yaml.warnings({'YAMLLoadWarning': False})

uid = random.randint(1, 1000000)
# Load config.yaml
with open("config.yaml", "r") as f:
	cfg = yaml.load(f)

# Create List symbols
l_symbols = dict()
# Open log file
datafirst_log = open(cfg["logFileName"], "w+")
message_datachanged = "none"
msg_c = 1


def datachanged_on_message(client, userdata, message):
	msg = str(message.payload.decode("utf-8"))
	global msg_c
	msg_c += 1
	# print(msg)
	# parse json
	msg_json = json.loads(msg)
	for key in msg_json["symbols"].keys():
		l_symbols[key] = msg


def main():
	c_datachanged = mqtt.Client(cfg["dataInfo"]["datachanged_queue_name"] + "_" + str(uid))
	c_datachanged.username_pw_set(cfg["serverInfo"]["username"], cfg["serverInfo"]["password"])
	c_datachanged.on_message = datachanged_on_message
	c_datachanged.connect(cfg["serverInfo"]["host"], int(cfg["serverInfo"]["port"]))

	c_datachanged.subscribe(cfg["dataInfo"]["datachanged_bind_Key"], int(cfg["dataInfo"]["datachanged_qos"]))
	print("Subcriber {} Success\n".format(cfg["dataInfo"]["datachanged_bind_Key"]))
	print("Connect Success! Begin listen....\n")
	c_datachanged.loop_start()

	while 1:
		# a = 1
		datafirst_log.seek(0)
		for key in l_symbols.keys():
			datafirst_log.write(l_symbols[key] + "\r\n")
		datafirst_log.write(str(msg_c) + "\r\n")

		# print("%%%%%%%%%%%%%%%\n")
		# pp.pprint(l_symbols)
		# print("\n%%%%%%%%%%%%%%%\n")
		# datafirst_log.write(msg)
		# time.sleep(1)

	print("Close!\n")
	datafirst_log.close()


main()



