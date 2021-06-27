import DBConnection as DB
import pandas as pd


class DataFood:
    DBConnection = None
    raw_recipes = None

    def __init__(self):
        self.raw_recipes = pd.read_csv("Food/RAW_recipes.csv")
        self.start_connection()

    def start_connection(self):
        self.DBConnection = DB.DBConnection()
        self.DBConnection.connect()

    def create_model_from_file(self, sql_file_path="../SQL/FoodDB.sql"):
        self.DBConnection.execute_query_from_file(sql_file_path)
        self.DBConnection.commit()