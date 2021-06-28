import json

import DBConnection as DB
import pandas as pd


class DataFood:
    DBConnection = None
    raw_recipes = None

    def __init__(self):
        print("Loading and unpacking raw data.")
        self.raw_recipes = pd.read_csv("Food/RAW_recipes.csv", converters={'tags': eval, 'steps': eval, 'ingredients': eval})
        self.start_connection()

    def start_connection(self):
        print("Attempting to connect to database env.")
        self.DBConnection = DB.DBConnection(config_file="config.json")
        self.DBConnection.connect()

    def create_model_from_file(self, sql_file_path="SQL/FoodDB.sql"):
        print(f"Attempting to create model from file {sql_file_path}.")
        self.DBConnection.execute_query_from_file(sql_file_path)
        self.DBConnection.commit()
        print("Model has been created successfully.\n")

    def clean_recipes_dataset(self):
        print("Cleaning recipes data.")
        print("Removing empty rows.")
        self.raw_recipes = self.raw_recipes.dropna()
        print("Removing faulty submission dates.")
        self.raw_recipes["submitted"] = pd.to_datetime(self.raw_recipes["submitted"], errors='coerce')
        self.raw_recipes.dropna(subset=['submitted'], inplace=True)
        self.raw_recipes["submitted"] = self.raw_recipes["submitted"].dt.date
        print("Recipes data cleaned successfully.\n")

    def load_contributors(self):
        print("Extracting users from raw data.")
        print("Cleaning users from raw data.")
        contributors = self.raw_recipes["contributor_id"].dropna().unique()
        print("Inserting users data into the database.")
        for user in contributors:
            insert_query = f'use Food; insert into users(user_id) values({user});'
            self.DBConnection.execute_query(insert_query)
        self.DBConnection.commit()
        print("User data successfully committed to the database.\n")

    def load_tags(self):
        print("Extracting tags from raw data.")
        print("Cleaning tags from raw data.")
        tags = self.raw_recipes["tags"].explode().dropna().unique()
        print("Inserting tags data into the database.")
        for tag in tags:
            insert_query = f'use Food; insert into tags(tag_name) values({json.dumps(tag)});'
            self.DBConnection.execute_query(insert_query)
        self.DBConnection.commit()
        print("Tags data successfully committed to the database.\n")

    def load_steps(self):
        print("Extracting steps from raw data.")
        print("Cleaning steps from raw data.")
        steps = self.raw_recipes["steps"].explode().dropna().unique()
        print("Inserting steps data into the database.")
        for step in steps:
            insert_query = f'use Food; insert into steps(step_name) values({json.dumps(step)});'
            self.DBConnection.execute_query(insert_query)
        self.DBConnection.commit()
        print("Steps data successfully committed to the database.\n")

    def load_ingredients(self):
        print("Extracting ingredients from raw data.")
        print("Cleaning ingredients from raw data.")
        ingredients = self.raw_recipes["ingredients"].explode().dropna().unique()
        print("Inserting ingredients data into the database.")
        for ingredient in ingredients:
            insert_query = f'use Food; insert into ingredients(ingredient_name) values({json.dumps(ingredient)});'
            self.DBConnection.execute_query(insert_query)
        self.DBConnection.commit()
        print("Ingredients successfully committed to the database.\n")

    def add_recipe_row(self, row):
        insert_query = f'use Food; insert into recipes values({row["id"]}, {json.dumps(row["name"])}, {row["minutes"]},' \
                       f' "{row["submitted"]}", {row["contributor_id"]}, {json.dumps(row["description"])}, {row["n_steps"]}, {row["n_ingredients"]});'
        self.DBConnection.execute_query(insert_query)

    def load_recipes(self):
        print("Extracting recipes from raw data.")
        print("Inserting recipes data into the database.")
        self.raw_recipes.apply(lambda row: self.add_recipe_row(row), axis=1)
        self.DBConnection.commit()
        print("Recipes successfully committed to the database.\n")

    def add_recipe_tag_row(self, row):
        recipe_id = row["id"]
        tags = row["tags"]
        for tag in tags:
            if tag:
                insert_query = f'use Food; insert into recipes_tags values ({recipe_id}, (select tag_id from tags where tag_name = {json.dumps(tag)}));'
                self.DBConnection.execute_query(insert_query)

    def load_recipes_tags(self):
        print("Extracting recipes-tags from raw data.")
        print("Inserting recipes-tags data into the database.")
        self.raw_recipes.apply(lambda row: self.add_recipe_tag_row(row), axis=1)
        self.DBConnection.commit()
        print("Recipes-tags successfully committed to the database.\n")

    def add_recipe_ingredient_row(self, row):
        recipe_id = row["id"]
        ingredients = row["ingredients"]
        for ingredient in ingredients:
            if ingredient:
                insert_query = f'use Food; insert into recipes_ingredients values ({recipe_id}, (select ingredient_id from ingredients where ingredient_name = {json.dumps(ingredient)}));'
                self.DBConnection.execute_query(insert_query)

    def load_recipes_ingredients(self):
        print("Extracting recipes-ingredients from raw data.")
        print("Inserting recipes-ingredients data into the database.")
        self.raw_recipes.apply(lambda row: self.add_recipe_ingredient_row(row), axis=1)
        self.DBConnection.commit()
        print("Recipes-ingredients successfully committed to the database.\n")

    def add_recipe_step_row(self, row):
        recipe_id = row["id"]
        steps = row["steps"]
        for step in steps:
            if step:
                insert_query = f'use Food; insert into recipes_steps values ({recipe_id}, (select step_id from steps where step_name = {json.dumps(step)}));'
                self.DBConnection.execute_query(insert_query)

    def load_recipes_steps(self):
        print("Extracting recipes-steps from raw data.")
        print("Inserting recipes-steps data into the database.")
        self.raw_recipes.apply(lambda row: self.add_recipe_step_row(row), axis=1)
        self.DBConnection.commit()
        print("Recipes-steps successfully committed to the database.\n")