import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
import src.utils as utils
import time
import src.DBMS as mongodb
import psycopg2.sql as sql
from psycopg2.extensions import adapt
from psycopg2.extensions import AsIs


class PostgreDB:

    def __init__(self):

        start_time = time.time()

        self.config_data = utils.get_config_data()

        self.DB_NAME, self.USER, self.PASSWORD, self.HOST, self.PORT, self.DEFAULT_DB = self.init_config()

        try:

            # Connect to default db first
            self.temp_connection = psycopg2.connect(database=self.DEFAULT_DB, user=self.USER, password=self.PASSWORD,
                                               host=self.HOST, port=self.PORT)
            self.temp_connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            print("Connected!")

            init_time = time.time() - start_time
            print("Init time = ", init_time)

            self.create_db()
            self.connection = self.connect()

        except psycopg2.DatabaseError as error:

            print(error)

        self.attributes = utils.get_keys()
        # print(self.attributes)
        print("Constructor done!")
        print("Construction time = ", time.time() - start_time)

    def create_db(self):

        start = time.time()

        try:

            cursor = self.temp_connection.cursor()
            cursor.execute("""
                            CREATE DATABASE postgresdb; 
                            """)

            print("Database creation time = ", time.time() - start)

        except psycopg2.Error as error:
            print("Database creation fail time = ", time.time() - start)
            print(error)

        finally:
            cursor.close()

    def connect(self):
        start = time.time()
        print("Connecting!")

        try:

            connection = psycopg2.connect(database=self.DB_NAME, user=self.USER, password=self.PASSWORD,
                                            host=self.HOST, port=self.PORT)
            connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            print("Connection successful!")

            print("Database connection time =", time.time() - start)

        except psycopg2.Error as error:

            print(error)
            print("Database connection fail time =", time.time() - start)

        finally:
            return connection

    # Get connection info from config
    def init_config(self):

        DB_NAME = utils.get_info(self.config_data, "POSTGRES_DB_NAME")
        USER = utils.get_info(self.config_data, "POSTGRES_USER")
        PASSWORD = utils.get_info(self.config_data, "POSTGRES_PASSWORD")
        HOST = utils.get_info(self.config_data, "POSTGRES_HOST")
        PORT = utils.get_info(self.config_data, "POSTGRES_PORT")
        DEFAULT = utils.get_info(self.config_data, "POSTGRES_DEFAULT")

        return DB_NAME, USER, PASSWORD, HOST, PORT, DEFAULT

    # Create table
    def create_table(self):

        table_name = utils.get_info(self.config_data, "COLLECTION_NAME")
        cursor = self.connection.cursor("""
                                        DROP TABLE IF EXISTS Requests CASCADE ;
                                        """)

        try:

            cursor.execute()

            cursor.execute("""
                            CREATE TABLE IF NOT EXISTS Requests (
                            _id SERIAL PRIMARY KEY
                            )
                            """,)

            print("TABLE CREATED!")
        except (Exception, psycopg2.DatabaseError) as error:
            print(error)
            print("TABLE COULD NOT BE CREATED!")

    # Read database from mongodb and create a table importing the data
    def read_mongodb(self):

        collection_name = utils.get_info(self.config_data, "COLLECTION_NAME")
        mongo_db = mongodb.DBMS(collection_name)
        cursor = self.connection.cursor()
        self.attributes = utils.transform_types(self.attributes)

        for key, value in self.attributes.items():

            cursor.execute(sql.SQL('ALTER TABLE Requests '
                                    'ADD COLUMN IF NOT EXISTS %s %s ;'), (AsIs(key), AsIs(value), ))

    def create_tables_example(self):

        start = time.time()
        commands = (
            """
            DROP TABLE IF EXISTS vendors CASCADE ;
            """,
            """
            DROP TABLE IF EXISTS parts CASCADE;
            """,
            """
            DROP TABLE IF EXISTS part_drawings CASCADE;
            """,
            """
            DROP TABLE IF EXISTS vendor_parts CASCADE;
            """,
            """
            CREATE TABLE IF NOT EXISTS vendors (
                vendor_id SERIAL PRIMARY KEY,
                vendor_name VARCHAR(255) NOT NULL
            )
            """,
            """ 
            CREATE TABLE IF NOT EXISTS parts (
                part_id SERIAL PRIMARY KEY,
                part_name VARCHAR(255) NOT NULL
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS part_drawings (
                part_id INTEGER PRIMARY KEY,
                file_extension VARCHAR(5) NOT NULL,
                drawing_data BYTEA NOT NULL,
                FOREIGN KEY (part_id)
                REFERENCES parts (part_id)
                ON UPDATE CASCADE ON DELETE CASCADE
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS vendor_parts (
                vendor_id INTEGER NOT NULL,
                part_id INTEGER NOT NULL,
                PRIMARY KEY (vendor_id , part_id),
                FOREIGN KEY (vendor_id)
                    REFERENCES vendors (vendor_id)
                    ON UPDATE CASCADE ON DELETE CASCADE,
                FOREIGN KEY (part_id)
                    REFERENCES parts (part_id)
                    ON UPDATE CASCADE ON DELETE CASCADE
            )
            """)

        try:
            cursor = self.connection.cursor()
            for command in commands:
                cursor.execute(command)

            cursor.close()
            print("Tables created! ")

        except (Exception, psycopg2.DatabaseError) as Error:
            print("Tables couldn't be created! ")
            print(Error)

        finally:

            print("Table example creation time =", time.time() - start)



db = PostgreDB()
db.create_table()
db.read_mongodb()
# test()

