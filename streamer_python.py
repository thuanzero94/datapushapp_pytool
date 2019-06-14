import paho.mqtt.client as mqtt
import time, sys
import yaml
import json
import random
import pprint

# pp = pprint.PrettyPrinter(indent=2) #print pretty json
# yaml.warnings({'YAMLLoadWarning': False})

uid = random.randint(1, 1000000)
# Load config.yaml
with open("config.yaml", "r") as f:
	cfg = yaml.load(f)

# Create List symbols
l_symbols = dict()
# Open log file
log_file = open(cfg["logFileName"], "w+")
message_datachanged = "none"


def my_print(d):
	if int(cfg["debug"]) == 1:
		print(d)


def datachanged_on_message(client, userdata, message):
	msg = str(message.payload.decode("utf-8"))
	my_print(msg)
	# parse json
	msg_json = json.loads(msg)
	for key in msg_json["symbols"].keys():
		l_symbols[key] = msg
	# Write to file
	log_file.seek(0)
	for key in l_symbols.keys():
		log_file.write(l_symbols[key] + "\r\n")


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
		# print("%%%%%%%%%%%%%%%\n")
		# pp.pprint(l_symbols)
		# print("\n%%%%%%%%%%%%%%%\n")
		# datafirst_log.write(msg)
		time.sleep(0.1)

	print("Close!\n")
	log_file.close()


main()



