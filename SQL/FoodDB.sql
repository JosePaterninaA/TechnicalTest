DROP DATABASE IF EXISTS Food;
CREATE DATABASE Food;
USE Food;

CREATE TABLE users (
    user_id INT(10),
    PRIMARY KEY (user_id)
);

CREATE TABLE recipes (
    recipe_id INT(10),
    name VARCHAR(100),
    minutes INT(4),
    submitted DATE,
    contributor_id INT(10),
    description VARCHAR(10000),
    n_steps INT,
    n_ingredients INT,
    FOREIGN KEY (contributor_id) REFERENCES users(user_id),
    PRIMARY KEY (recipe_id)
);

CREATE TABLE interactions (
    interaction_id INT AUTO_INCREMENT,
    user_id INT(10),
    recipe_id INT(10),
    date DATE,
    rating INT,
    review VARCHAR(2000),
    FOREIGN KEY (user_id) REFERENCES users(user_id),
    FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
    PRIMARY KEY (interaction_id)
);

CREATE TABLE tags(
    tag_id INT(10) AUTO_INCREMENT,
    tag_name VARCHAR(1000),
    PRIMARY KEY (tag_id)
);

CREATE TABLE recipes_tags(
    recipe_id INT(10),
    tag_id INT(10),
    FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
    FOREIGN KEY (tag_id) REFERENCES tags(tag_id)
);

CREATE TABLE ingredients(
    ingredient_id INT(10) AUTO_INCREMENT,
    ingredient_name VARCHAR(1000),
    PRIMARY KEY (ingredient_id)
);

CREATE TABLE recipes_ingredients(
    recipe_id INT(10),
    ingredient_id INT(10),
    FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
    FOREIGN KEY (ingredient_id) REFERENCES ingredients(ingredient_id)
);

CREATE TABLE steps(
    step_id INT(10) AUTO_INCREMENT,
    step_name VARCHAR(1000),
    PRIMARY KEY (step_id)
);

CREATE TABLE recipes_steps(
    recipe_id INT(10),
    step_id INT(10),
    FOREIGN KEY (recipe_id) REFERENCES recipes(recipe_id),
    FOREIGN KEY (step_id) REFERENCES steps(step_id)
);