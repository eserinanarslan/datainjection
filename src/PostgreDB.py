import psycopg2
import json
from src.DBMS import DBMS as dbms
from src.FileReader import FileReader as fr
from bson import Code
from pymongo import MongoClient
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT


class PostgreDB:

    def __init__(self):

        self.config_data = self.get_configData()

        self.DB_NAME, self.USER, self.PASSWORD, self.HOST, self.PORT = self.init_config()

        try:

            self.connection = psycopg2.connect(database=self.DB_NAME, user=self.USER, password=self.PASSWORD,
                                               host=self.HOST, port=self.PORT)
            print("Connection successful!")
            self.connection.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
            self.cursor = self.connection.cursor()

            """
            query = "SELECT 'CREATE DATABASE "
            query += "'" + self.DB_NAME
            query += "'\nWHERE NOT EXISTS(SELECT FROM pg_database WHERE datname = "
            query += "'" + self.DB_NAME + "'"
            query += ")\gexec"

            self.cursor.execute("IF EXISTS (SELECT FROM pg_database WHERE datname = %s ;)" + self.DB_NAME)

            self.cursor.execute("CREATE DATABASE %s  ;" % self.DB_NAME_name)
            """

            self.create_tables_example()

        except psycopg2.DatabaseError as error:

            print(error)

        # print(self.DB_NAME, self.HOST, self.PASSWORD, self.PORT, self.USER)

        self.attributes = self.get_attributes()
        # print(self.attributes)
        print("Constructor done!")

    def create_DB(self):
        self.cursor.execute("CREATE DATABASE %s  ;" % self.DB_NAME_name)
        return

    # Get connection info from config
    def init_config(self):

        DB_NAME = self.get_info("POSTGRES_DB_NAME")
        USER = self.get_info("POSTGRES_USER")
        PASSWORD = self.get_info("POSTGRES_PASSWORD")
        HOST = self.get_info("POSTGRES_HOST")
        PORT = self.get_info("POSTGRES_PORT")

        return DB_NAME, USER, PASSWORD, HOST, PORT

    # Get config data from the config file
    def get_configData(self):
        config_location = "config/config.json"
        config_file = open(config_location, "r")
        config_data = config_file.read()
        return config_data

    # Get wanted info from config data
    def get_info(self, config_key):
        data = json.loads(self.config_data)
        field = str(data[config_key])

        return field

    def get_attributes(self):

        fileReader = fr()

        configData = fileReader.getConfigData()

        client = MongoClient(fileReader.getInfo("CLIENT_URL"))

        db = client[fileReader.getInfo("DB_NAME")]
        collectionName = fileReader.getInfo("COLLECTION_NAME")

        map = Code("function() { for (var key in this) { emit(key, null); } }")
        reduce = Code("function(key, stuff) { return null; }")
        result = db[collectionName].map_reduce(map, reduce, "Attributes")

        attributes = {}

        for attribute in result.distinct('_id'):
            attributes[attribute] = type(attribute)

        print(attributes)
        return attributes

    def create_tables_example(self):
        commands = (
            """
            CREATE TABLE IF NOT EXISTS vendors (
            vendor_id SERIAL PRIMARY KEY,
            vendor_name VARCHAR(255) NOT NULL
        )
            """,
            """ CREATE TABLE IF NOT EXISTS parts (
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
            for command in commands:
                self.cursor.execute(command)

            self.cursor.close()
            self.connection.commit()
            print("Tables created! ")

        except (Exception, psycopg2.DatabaseError) as Error:
            print(Error)

        finally:

            if self.connection is not None:
                self.connection.close()

    def manual_attributes(self):
        attributes = ["accounts", "action", "client_ip", "controller", "device", "devices", "divID", "dwh",
                      "email", "environment", "file_location", "filter", "host", "method", "module",
                      "oauth_proxy_redirect_host", "path", "referrer", "request_id", "token", "type", "user_id",
                      "user_name", "username"]


DB = PostgreDB()
