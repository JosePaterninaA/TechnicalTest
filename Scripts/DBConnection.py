import mysql.connector as connector
from mysql.connector import errorcode
import json


class DBConnection:
    CONNECTION = None
    CURSOR = None
    HOST = None
    USER = None
    PASSWORD = None
    DATABASE = None

    def __init__(self, host='localhost', user='root', password='', database='', config_file='../config.json'):
        if config_file:
            with open(config_file, "r") as config_file:
                config_json = json.load(config_file)
                print("Configuration has succeeded.")
            local_config = config_json["CONFIG"]
            self.HOST = local_config["HOST"]
            self.USER = local_config["USER"]
            self.PASSWORD = local_config["PASSWORD"]
            self.DATABASE = local_config["DATABASE"]
            pass
        else:
            self.HOST = host
            self.USER = user
            self.PASSWORD = password
            self.DATABASE = database

    def connect(self):
        try:
            CON = connector.connect(
                host=self.HOST,
                user=self.USER,
                password=self.PASSWORD
            )
            self.CONNECTION = CON
            self.CURSOR = CON.cursor()
            print("Connection has succeeded.")

        except connector.Error as error:
            if error.errno == errorcode.ER_ACCESS_DENIED_ERROR:
                print("Access denied. Check credentials.")
            else:
                print(error.msg)
                print("Unknown error.")
