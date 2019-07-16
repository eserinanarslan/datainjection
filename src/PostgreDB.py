import psycopg2
import json


class PostgreDB:

    def __init__(self):

        self.initConfig()

        try:

            self.connection = psycopg2.connect(database=self.DB_NAME, user=self.USER, password=self.PASSWORD, host=self.HOST, port=self.PORT)
            print("Connection successful!")
            self.cursor = self.connection.cursor

        except psycopg2.DatabaseError as error:

            print(error)

        print(self.DB_NAME, self.HOST, self.PASSWORD, self.PORT, self.USER)
        print("Constructor done!")

    # Get connection info from config
    def initConfig(self):

        self.configData = self.getConfigData()

        self.DB_NAME = self.getInfo("POSTGRES_DB_NAME")
        self.USER = self.getInfo("POSTGRES_USER")
        self.PASSWORD = self.getInfo("POSTGRES_PASSWORD")
        self.HOST = self.getInfo("POSTGRES_HOST")
        self.PORT = self.getInfo("POSTGRES_PORT")

    # Get config data from the config file
    def getConfigData(self):
        self.configLocation = "config/config.json"
        self.configFile = open(self.configLocation, "r")
        configData = self.configFile.read()
        return configData

    # Get wanted info from config data
    def getInfo(self, configKey):
        data = json.loads(self.configData)
        field = str(data[configKey])

        return field


DB = PostgreDB()

