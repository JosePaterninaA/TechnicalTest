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

    def close(self):
        self.CURSOR.close()
        self.CONNECTION.close()

    def execute_query(self, query, multi):
        results = self.CURSOR.execute(query, multi=True)
        for result in results:
            if result.with_rows:
                print("Rows produced by statement '{}':".format(
                    result.statement))
                print(result.fetchall())
            else:
                print("Number of rows affected by statement '{}': {}".format(
                    result.statement, result.rowcount))
        print("Query execution succeeded.")

    def execute_query_from_file(self, file_path):
        try:
            with open(file_path) as file:
                results = self.CURSOR.execute(file.read(), multi=True)
                for result in results:
                    if result.with_rows:
                        print("Rows produced by statement '{}':".format(
                            result.statement))
                        print(result.fetchall())
                    else:
                        print("Number of rows affected by statement '{}': {}".format(
                            result.statement, result.rowcount))
                print("Query execution succeeded.")
        except FileNotFoundError:
            print('File could not be open.')

    def commit(self):
        self.CONNECTION.commit()
