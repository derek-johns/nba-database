import mysql.connector
from mysql.connector import Error
import pandas as pd

PW = 'cannon'

# Create connection to database server
def connect_to_server(host_name, user_name, user_password):
    connection = None
    try:
        connection = mysql.connector.connect(host=host_name,
                                             user=user_name,
                                             passwd=user_password)
        print('MySQL Database connection successful')
    except Error as err:
        print(f'Error: "{err}"')

    return connection





# Create a database
def create_database(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        print('Database created successfully')
    except Error as err:
        print(f'Error: "{err}"')


# Create database connection
def create_db_connection(host_name, user_name, user_password, db_name):
    connection = None
    try:
        connection = mysql.connector.connect(host=host_name,
                                             user=user_name,
                                             passwd=user_password,
                                             database=db_name)
        print('MySQL Database connection successful')
    except Error as err:
        print(f'Error: "{err}"')

    return connection


# Query execution
def execute_query(connection, query):
    cursor = connection.cursor()
    try:
        cursor.execute(query)
        connection.commit()
        print('Query successful')
    except Error as err:
        print(f'Error: "{err}"')


# Create tables
create_player_table = """
CREATE TABLE player (
    player_id INT PRIMARY KEY,
    first_name VARCHAR(50) NOT NULL,
    last_name VARCHAR(50) NOT NULL,
    
);
"""


# Program execution
conn = connect_to_server('localhost', 'root', PW)
create_database_query = 'CREATE DATABASE nba'
create_database(conn, create_database_query)
