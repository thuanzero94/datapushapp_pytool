import paho.mqtt.client as mqtt
import time, sys
import yaml
import json
import random
import pprint
from configparser import ConfigParser
from mysql.connector import MySQLConnection, Error
import threading
import datetime


def read_db_config(filename='config.ini', section='sql_info'):
    """ Read database configuration file and return a dictionary object
    :param filename: name of the configuration file
    :param section: section of database configuration
    :return: a dictionary of database parameters
    """
    # create parser and read ini configuration file
    parser = ConfigParser()
    parser.read(filename)

    # get section, default to serverInfo
    db = {}
    if parser.has_section(section):
        items = parser.items(section)
        for item in items:
            db[item[0]] = item[1]
    else:
        raise Exception('{0} not found in the {1} file'.format(section, filename))

    return db


uid = time.time()
logInfo = read_db_config(section='logInfo')
severInfo = read_db_config(section='serverInfo')
dataInfo = read_db_config(section='dataInfo')
sqlOption = read_db_config(section='sql_option')
DELETE_TIME_INTERVAL = sqlOption['delete_time_interval']
DELETE_LIMIT = sqlOption['delete_limit']
# print(severInfo)
# print(dataInfo)
# print(logInfo)

db_config = read_db_config(section='sql_info')
# conn = MySQLConnection(**db_config)


def my_print(d):
    try:
        level = int(logInfo["level"])
    except:
        print("don't have or wrong type 'debug' option, please check config.yaml!")
        exit(-1)
    if level == 1:
        print(d)


def query_book(conn, f, symbol, book='quote', type='insert'):
    if book == 'quote':
        if type == 'insert':
            query = "INSERT INTO quote(`symbol`, `bid`, `last`, `ask`, `change`, `high`, `low`, `open`, `prev_close`, `time`, `timestamp`)" \
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), UNIX_TIMESTAMP())"
            val = (symbol, f['bid'], f['last'], f['ask'], f['change'], f['high'], f['low'], f['open'], f['close'])
        elif type == 'update':
            query = "UPDATE quote SET `bid`=%s, `last`=%s, `ask`=%s, `change`=%s, `high`=%s, `low`=%s, `open`=%s, `prev_close`=%s, `time`=NOW(), `timestamp`=UNIX_TIMESTAMP()" \
                    "WHERE `symbol`=%s"
            val = (f['bid'], f['last'], f['ask'], f['change'], f['high'], f['low'], f['open'], f['close'], symbol)
    elif book == 'quotelog':
        if type == 'insert':
            query = "INSERT INTO quotelog(`symbol`, `bid`, `last`, `ask`, `change`, `high`, `low`, `open`, `prev_close`, `time`, `timestamp`)" \
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW(), UNIX_TIMESTAMP())"
            val = (symbol, f['bid'], f['last'], f['ask'], f['change'], f['high'], f['low'], f['open'], f['close'])
    try:
        cursor = conn.cursor()
        cursor.execute(query, val)
        conn.commit()
    except Error as error:
        print(error)
        return 0
    finally:
        if cursor:
            cursor.close()
    return 1


class DeleteQuotelog(threading.Thread):
    def __init__(self, name, threadID):
        global sqlOption
        threading.Thread.__init__(self)
        self.name = name
        self.threadID = threadID
        self.time_start = time.time()
        self.DELETE_TIME_INTERVAL = sqlOption['delete_time_interval']
        self.MySQLConnection = MySQLConnection

    def run(self):
        self.time_start = time.time()
        print("\nStarting thread: " + self.name)
        self.print_date(self.name, self.threadID)
        # print("Exiting " + self.name)
        try:
            if not self.delete_quotelog():
                print('Delete quotelog failed')
        except:
            print 'delete quotelog failed'
        print('First Delete - $$$$$$$ Total delete time: {}'.format(time.time() - self.time_start))
        while True:
            current_time = time.time()
            if current_time - self.time_start > int(self.DELETE_TIME_INTERVAL) * 60:
                try:
                    if not self.delete_quotelog():
                        print('Delete quotelog failed')
                except:
                    print 'delete quotelog failed'
                self.time_start = time.time()
                print('$$$$$$$ Total delete time: {}'.format(self.time_start - current_time))
            time.sleep(0.5)

    def delete_quotelog(self):
        global db_config
        query = "SELECT priceid FROM quotelog WHERE timestamp < (UNIX_TIMESTAMP() - {} * 60)".format(
            self.DELETE_TIME_INTERVAL)
        try:
            connection = self.MySQLConnection(**db_config)
            cur = connection.cursor()
            cur.execute(query)
            result = cur.fetchall()
            print("$$$$$$$ Total Items from quotelog will delete: {}".format(len(result)))
            if len(result) > 2:
                # print('type: {} - {}\n {} - {}'.format(len(result), result, result[0][0], result[-1][0]))
                d_query = "DELETE FROM quotelog WHERE priceid BETWEEN {} and {}".format(result[0][0], result[-1][0])
                # print(d_query)
                cur.execute(d_query)
            #     for x in result:
            #         # print(x[0])
            #         d_query = "DELETE FROM quotelog WHERE priceid={}".format(x[0])
            #         # print(query)
            #         cur.execute(d_query)
                connection.commit()
        except Exception as e:
            print e
            return 0
        finally:
            connection.close()
            cur.close()
        return 1

    def print_date(self, thread_name, counter):
        datefields = []
        today = datetime.datetime.today()
        datefields.append(today)
        print("{}[{}]: {}".format(thread_name, counter, datefields[0]))


def update_database(f, symbol):
    global db_config
    # print(symbol)
    # print(f)
    query = "SELECT * FROM {} WHERE symbol ='{}'".format('quote', symbol)
    try:
        # db_config = read_db_config()
        conn = MySQLConnection(**db_config)
        cursor = conn.cursor()
        cursor.execute(query)
        if len(cursor.fetchall()) == 0:
            # print('Create New %s' % symbol)
            query_book(conn, f, symbol, 'quote', 'insert')
        else:
            # print('%s exist' % symbol)
            query_book(conn, f, symbol, 'quote', 'update')
        # query_book(conn, f, symbol, 'quote', 'update')

        # Insert to querylog table
        # for s in range(0, 1000):
        query_book(conn, f, symbol, 'quotelog', 'insert')
    except Error as e:
        print(e)
        return 0
    finally:
        if cursor:
            cursor.close()
        conn.close()


def datachanged_on_message(client, userdata, message):
    msg = str(message.payload.decode("utf-8"))
    my_print(msg)
    # parse json
    msg_json = json.loads(msg)
    # Get symbol key & data
    for key in msg_json['symbols'].keys():
        symbol_key = key
    symbol_data = msg_json['symbols'][symbol_key]
    # print(symbol_data)
    # print(symbol_key)
    # Update to database
    try:
        update_database(symbol_data, symbol_key)
    except:
        print('Update database Error')


def on_connect(client, userdata, flags, rc):
    sw_connection_result = {
        0: "Rabbit MQTT - Connection successful",
        1: "Rabbit MQTT - Connection refused - incorrect protocol version",
        2: "Rabbit MQTT - Connection refused - invalid client identifier",
        3: "Rabbit MQTT - Connection refused - server unavailable",
        4: "Rabbit MQTT - Connection refused - bad username or password",
        5: "Rabbit MQTT - Connection refused - not authorised"
    }

    print(sw_connection_result.get(rc, "Rabbit MQTT - Connection refused - Other failed"))
    if rc == 0:
        client.subscribe(dataInfo["datachanged_bind_key"], int(dataInfo["datachanged_qos"]))
        print("Subcribe '{}' Success".format(dataInfo["datachanged_bind_Key"]))


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Unexpected MQTT disconnection. Will auto-reconnect")


def main():
    # Create a mqtt client object
    try:
        c_datachanged = mqtt.Client(dataInfo["datachanged_queue_name"] + "_" + str(uid))
    except:
        print("Initial mqtt Client failed, Please check 'dataInfo' & 'datachanged_queue_name' options!")
        exit(-1)
    # Set username & password
    try:
        c_datachanged.username_pw_set(severInfo["username"], severInfo["password"])
    except:
        print("Set Username & Password failed, Please check 'serverInfo' & 'username' & 'password' options!")
        exit(-1)
    # Set the mqtt client other options
    c_datachanged.on_message = datachanged_on_message
    c_datachanged.on_disconnect = on_disconnect
    c_datachanged.on_connect = on_connect
    # Connect to Server
    try:
        c_datachanged.connect(severInfo["host"], int(severInfo["port"]))
    except:
        print("Connect to server failed, Please check 'host' & 'port' options!")
        exit(-1)

    # Start thread - delete quotelog
    DeleteQuotelog("Delete quotelog", 1).start()

    c_datachanged.loop_start()
    # c_datachanged.loop_forever()

    while 1:
        # print('a')
        time.sleep(0.5)

    print("Closed!")


if __name__ == "__main__":
    main()
