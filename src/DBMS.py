from pymongo import MongoClient
import json
import pymongo


class DBMS:

    def _init_(self, collectionName):

        self.configData = self.getConfigData()

        self.client = MongoClient(self.getInfo(self.configData, "CLIENT_URL"))

        self.DB = self.client[self.getInfo(self.configData, "DB_NAME")]

        # collections = self.DB.list_collection_names()
        # if collectionName in collections:
            # self.currentCollection = self.DB.get_collection(collectionName)

        self.currentCollection = self.DB[collectionName]

        index = self.currentCollection.create_index([("request_id", pymongo.ASCENDING)],
                                                             unique = True)
        sorted(list(self.DB.index_information()))

    # Get config data from the config file
    def getConfigData(self):

        configLocation = "../config\config.json"
        configFile = open(configLocation, "r")
        configData = configFile.read()
        # print(configData)
        return configData

    def getInfo(self, configData, configKey):

        data = json.loads(configData)
        # print(data)

        field = str(data[configKey])

        return field


