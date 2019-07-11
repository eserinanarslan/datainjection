from pymongo import MongoClient
import json
import pymongo.errors
import pymongo


class DBMS:

    def __init__(self, collectionName):

        self.configData = self.getConfigData()

        self.client = MongoClient(self.getInfo(self.configData, "CLIENT_URL"),
                                  username='myUserAdmin',
                                  password='abc123',)

        self.DB = self.client[self.getInfo(self.configData, "DB_NAME")]

        self.collections = self.DB.collection_names()
        if collectionName in self.collections:
            self.DB.drop_collection(collectionName)
            print("Collection dropped!")

        self.currentCollection = self.DB[collectionName]

        DB_INDEX = self.getInfo(self.configData, "DB_INDEX")
        self.currentCollection.create_index([(DB_INDEX, 1)], unique = True, background = True)

    # Insert document to the current collection
    def insertDocument(self, document):

        isDuplicate = False

        try:
            self.currentCollection.insert_one(document)

        except pymongo.errors.DuplicateKeyError:
            # print("Duplicate Request ID! ", document["request_id"])
            isDuplicate = True

        return isDuplicate

    # Get config data from the config file
    def getConfigData(self):

        configLocation = "config\config.json"
        configFile = open(configLocation, "r")
        configData = configFile.read()
        return configData

    # Get wanted info from config data
    def getInfo(self, configData, configKey):

        data = json.loads(configData)
        field = str(data[configKey])

        return field

    # Terminate DB connection
    def close(self):

        self.client.close()


# 3DB = DBMS("Requests")