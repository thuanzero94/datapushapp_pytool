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
import logging
from logging.handlers import RotatingFileHandler


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


whileLoop = True

uid = time.time()
logInfo = read_db_config(section='logInfo')
severInfo = read_db_config(section='serverInfo')
dataInfo = read_db_config(section='dataInfo')
sqlOption = read_db_config(section='sql_option')
DELETE_TIME_INTERVAL = sqlOption['delete_time_interval']
start_t = time.time()
# DELETE_LIMIT = sqlOption['delete_limit']
# print(severInfo)
# print(dataInfo)
# print(logInfo)

db_config = read_db_config(section='sql_info')
# conn = MySQLConnection(**db_config)

# Configure logging
# logging.basicConfig(filename=logInfo['log_file_name'], filemode='w', level=int(logInfo['level']), format='[%(asctime)s] p%(process)s|%(levelname)s|L_%(lineno)d: %(message)s')
log = logging.getLogger('root')
log.setLevel(int(logInfo['level']))
formatter = logging.Formatter('[%(asctime)s] p%(process)s|%(levelname)s|L_%(lineno)d: %(message)s')
handler = RotatingFileHandler(logInfo['log_file_name'], mode='w', maxBytes=int(logInfo['max_size']) * 1024 * 1024,
                              backupCount=1, encoding=None, delay=0)
handler.setFormatter(formatter)
# handler.setLevel(int(logInfo['level']))
log.addHandler(handler)


def query_book(conn, f, symbol, book='quote', type='insert'):
    if book == 'quote':
        if type == 'insert':
            query = "INSERT INTO quote(`symbol`, `bid`, `last`, `ask`, `change`, `high`, `low`, `open`, `prev_close`, `time`, `timestamp`)" \
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(), UNIX_TIMESTAMP())"
            val = (symbol, f['bid'], f['last'], f['ask'], f['change'], f['high'], f['low'], f['open'], f['close'])
        elif type == 'update':
            query = "UPDATE quote SET `bid`=%s, `last`=%s, `ask`=%s, `change`=%s, `high`=%s, `low`=%s, `open`=%s, `prev_close`=%s, `time`=UTC_TIMESTAMP(), `timestamp`=UNIX_TIMESTAMP()" \
                    "WHERE `symbol`=%s"
            val = (f['bid'], f['last'], f['ask'], f['change'], f['high'], f['low'], f['open'], f['close'], symbol)
    elif book == 'quotelog':
        if type == 'insert':
            query = "INSERT INTO quotelog(`symbol`, `bid`, `last`, `ask`, `change`, `high`, `low`, `open`, `prev_close`, `time`, `timestamp`)" \
                    "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, UTC_TIMESTAMP(), UNIX_TIMESTAMP())"
            val = (symbol, f['bid'], f['last'], f['ask'], f['change'], f['high'], f['low'], f['open'], f['close'])
    try:
        cursor = conn.cursor()
        cursor.execute(query, val)
        conn.commit()
    except Error as error:
        log.error(error)
        return 0
    finally:
        if cursor:
            cursor.close()
    return 1


class UpdateDatabase(threading.Thread):
    def __init__(self, name, f, symbol):
        threading.Thread.__init__(self)
        self.name = name
        self.data = f
        self.symbol = symbol
        self.time_start = time.time()
        self.sql = MySQLConnection

    def run(self):
        # log.info("thread {} started".format(self.name))
        try:
            if not self.update_database(self.data, self.symbol):
                log.error('update db failed')
        except Error as e:
            log.error(e)
            log.error('update db failed')
        finally:
            log.debug(
                'thread {}, Total time: {}'.format(self.name, time.time() - self.time_start))

    def update_database(self, f, symbol):
        global db_config
        # print(symbol)
        # print(f)
        query = "SELECT * FROM {} WHERE symbol ='{}'".format('quote', symbol)
        try:
            # db_config = read_db_config()
            conn = self.sql(**db_config)
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
            log.error(e)
            return 0
        finally:
            if cursor:
                cursor.close()
            conn.close()
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
        log.info("thread {}-{} started".format(self.name, self.threadID))
        try:
            if not self.delete_quotelog():
                log.error('Delete quotelog failed')
        except Error as e:
            log.error(e)
            log.error('delete quotelog failed')
        finally:
            log.info(
                'thread {}-{} Stopped. Total time: {}'.format(self.name, self.threadID, time.time() - self.time_start))

    def delete_quotelog(self):
        global db_config
        query = "SELECT priceid FROM quotelog WHERE timestamp < (UNIX_TIMESTAMP() - {} * 60)".format(
            self.DELETE_TIME_INTERVAL)
        try:
            connection = self.MySQLConnection(**db_config)
            cur = connection.cursor()
            cur.execute(query)
            result = cur.fetchall()
            log.info("$$$$$$$ Total Items from quotelog will delete: {}".format(len(result)))
            if len(result) > 2:
                # print('type: {} - {}\n {} - {}'.format(len(result), result, result[0][0], result[-1][0]))
                d_query = "DELETE FROM quotelog WHERE priceid BETWEEN {} and {}".format(result[0][0], result[-1][0])
                # print(d_query)
                cur.execute(d_query)
                connection.commit()
        except Exception as e:
            log.error(e)
            return 0
        finally:
            connection.close()
            cur.close()
        return 1



def datachanged_on_message(client, userdata, message):
    global start_t, DELETE_TIME_INTERVAL
    msg = str(message.payload.decode("utf-8"))
    log.debug(msg)
    # my_print(msg)
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
        # update_database(symbol_data, symbol_key)
        UpdateDatabase("Update quote", symbol_data, symbol_key).start()
        # Check Delete Quotelog
        current_t = time.time()
        if current_t - start_t > int(DELETE_TIME_INTERVAL) * 60:
            DeleteQuotelog("Delete quotelog", 1).start()
            start_t = time.time()
    except Exception as e:
        log.error(e)
        log.error('Update database Error')


def on_connect(client, userdata, flags, rc):
    sw_connection_result = {
        0: "Rabbit MQTT - Connection successful",
        1: "Rabbit MQTT - Connection refused - incorrect protocol version",
        2: "Rabbit MQTT - Connection refused - invalid client identifier",
        3: "Rabbit MQTT - Connection refused - server unavailable",
        4: "Rabbit MQTT - Connection refused - bad username or password",
        5: "Rabbit MQTT - Connection refused - not authorised"
    }

    log.info(sw_connection_result.get(rc, "Rabbit MQTT - Connection refused - Other failed"))
    if rc == 0:
        client.subscribe(dataInfo["datachanged_bind_key"], int(dataInfo["datachanged_qos"]))
        log.info("Subcribe '{}' Success".format(dataInfo["datachanged_bind_Key"]))


def on_disconnect(client, userdata, rc):
    if rc != 0:
        log.error("Unexpected MQTT disconnection. Will auto-reconnect")


def main():
    print("Streamer running, log write to {} ......".format(logInfo['log_file_name']))
    # Create a mqtt client object
    try:
        c_datachanged = mqtt.Client(dataInfo["datachanged_queue_name"] + "_" + str(uid))
    except Error as e:
        log.error(e)
        log.error("Initial mqtt Client failed, Please check 'dataInfo' & 'datachanged_queue_name' options!")
        exit(-1)
    # Set username & password
    try:
        c_datachanged.username_pw_set(severInfo["username"], severInfo["password"])
    except Error as e:
        log.error(e)
        log.error("Set Username & Password failed, Please check 'serverInfo' & 'username' & 'password' options!")
        exit(-1)
    # Set the mqtt client other options
    c_datachanged.on_message = datachanged_on_message
    c_datachanged.on_disconnect = on_disconnect
    c_datachanged.on_connect = on_connect
    # Connect to Server
    try:
        c_datachanged.connect(severInfo["host"], int(severInfo["port"]))
    except Error as e:
        log.error(e)
        log.error("Connect to server failed, Please check 'host' & 'port' options!")
        exit(-1)

    # Start thread - delete quotelog
    DeleteQuotelog("Delete quotelog", 1).start()

    c_datachanged.loop_start()
    # c_datachanged.loop_forever()

    global whileLoop, DELETE_TIME_INTERVAL

    while whileLoop:
        # print('a')
        try:
            time.sleep(0.2)
        except KeyboardInterrupt:
            log.info("User Ctrl + C. Closing program...!")
            whileLoop = False
            c_datachanged.disconnect()

    log.info("Closed!")


if __name__ == "__main__":
    main()
