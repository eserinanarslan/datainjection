from pymongo import MongoClient
import json
import pymongo.errors
import pymongo


class DBMS:

    def __init__(self, collectionName):

        self.configData = self.getConfigData()

        self.client = MongoClient(self.getInfo(self.configData, "CLIENT_URL"))

        self.DB = self.client[self.getInfo(self.configData, "DB_NAME")]

        # collections = self.DB.list_collection_names()
        # if collectionName in collections:
            # self.currentCollection = self.DB.get_collection(collectionName)

        self.currentCollection = self.DB[collectionName]

        indexName = "Index1"
        self.currentCollection.create_index([("request_id", 1)], unique = True, background = True)
        # sorted(list(self.DB.currentCollection.index_information()))

    # Insert document to the current collection
    def insertDocument(self, document):

        try:
            self.currentCollection.insert_one(document)

        except pymongo.errors.DuplicateKeyError:

            print("Duplicate Request ID! ", document["request_id"])

    # Get config data from the config file
    def getConfigData(self):

        configLocation = "../config\config.json"
        configFile = open(configLocation, "r")
        configData = configFile.read()
        return configData

    def getInfo(self, configData, configKey):

        data = json.loads(configData)

        field = str(data[configKey])

        return field


