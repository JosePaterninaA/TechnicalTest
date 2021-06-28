import DBConnection as DB
import pandas as pd
import json

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

    def load_contributors(self):
        contributors = self.raw_recipes["contributor_id"].dropna().unique()
        for user in contributors:
            insert_query = f'use Food; insert into users(user_id) values({user});'
            self.DBConnection.execute_query(insert_query, multi=True)
        self.DBConnection.commit()

    def load_tags(self):
        tags = self.raw_recipes["tags"].explode().dropna().unique()
        for tag in tags:
            insert_query = f'use Food; insert into tags(tag_name) values({json.dumps(tag)});'
            self.DBConnection.execute_query(insert_query, multi=True)
        self.DBConnection.commit()

    def load_steps(self):
        steps = self.raw_recipes["steps"].explode().dropna().unique()
        for step in steps:
            insert_query = f'use Food; insert into steps(step_name) values({json.dumps(step)});'
            self.DBConnection.execute_query(insert_query, multi=True)
        self.DBConnection.commit()

    def load_ingredients(self):
        ingredients = self.raw_recipes["ingredients"].explode().dropna().unique()
        for ingredient in ingredients:
            insert_query = f'use Food; insert into ingredients(ingredient_name) values({json.dumps(ingredient)});'
            self.DBConnection.execute_query(insert_query, multi=True)
        self.DBConnection.commit()

    def add_recipe_row(self, row):
        insert_query = f'use Food; insert into recipes values({row["id"]}, {json.dumps(row["name"])}, {row["minutes"]},' \
                       f' "{row["submitted"]}", {row["contributor_id"]}, {json.dumps(row["description"])}, {row["n_steps"]}, {row["n_ingredients"]});'
        self.DBConnection.execute_query(insert_query, multi=True)
        pass

    def load_recipes(self):
        self.raw_recipes.apply(lambda row: self.add_recipe_row(row), axis=1)
        self.DBConnection.commit()
        pass
