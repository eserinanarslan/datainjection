from pymongo import MongoClient
import json
import pymongo.errors
import pymongo
import time


class DBMS:

    def __init__(self, collectionName):

        self.collectionName = collectionName

        self.configData = self.getConfigData()

        self.client = MongoClient(self.getInfo("CLIENT_URL"),
                                  username=self.getInfo("USER_NAME"),
                                  password=self.getInfo("PASSWORD"),)

        self.DB = self.client[self.getInfo("DB_NAME")]

        self.duplicate_col_name = self.getInfo("COLLECTION_DUPLICATE")

        self.collections = self.DB.collection_names()
        if collectionName in self.collections:
            self.DB.drop_collection(collectionName)
            print("Collection dropped!")

        if self.duplicate_col_name in self.collections:
            self.DB.drop_collection(self.duplicate_col_name)

        self.currentCollection = self.DB[collectionName]

        # self.duplicate_collection = self.DB[self.duplicate_col_name]

        # DB_INDEX_1 = self.getInfo("DB_INDEX_1")
        # DB_INDEX_2 = self.getInfo("DB_INDEX_2")
        # DB_INDEX_3 = self.getInfo("DB_INDEX_3")

        # self.currentCollection.create_index([(DB_INDEX_1, 1), (DB_INDEX_2, 1), (DB_INDEX_3, 1)], unique=True, background=True)

        self.BATCH_SIZE = int(self.getInfo("BATCH_SIZE"))
        self.TIME_RESET = int(self.getInfo("TIME_RESET"))

    # Reset database connection
    # TODO
    def resetConnection(self, collectionName):

        self.client = MongoClient(self.getInfo("CLIENT_URL"),
                                  username=self.getInfo("USER_NAME"),
                                  password=self.getInfo("PASSWORD"), )

        self.DB = self.client[self.getInfo("DB_NAME")]

        self.collections = self.DB.collection_names()
        self.currentCollection = self.DB[collectionName]

        DB_INDEX = self.getInfo("DB_INDEX")
        self.currentCollection.create_index([(DB_INDEX, 1)], unique=True, background=True)

    # Insert document to the current collection
    def insertDocument(self, document):

        isDuplicate = False

        try:
            self.currentCollection.insert_one(document)

        except pymongo.errors.DuplicateKeyError:
            isDuplicate = True

        return isDuplicate

    # Insert a list of documents unordered
    def insertDocuments(self, documents):

        startingTime = time.time()

        resetCounter = 0

        duplicateCount = 0

        for i in range(0, (documents.__len__() // self.BATCH_SIZE) + 1):
            if (time.time() - startingTime) > (resetCounter + 1) * self.TIME_RESET:
                # self.resetConnection(self.collectionName)
                resetCounter += 1
                # print("100 sec")

            try:
                if (i + 1) * self.BATCH_SIZE <= documents.__len__():
                    self.currentCollection.insert_many(documents[i * self.BATCH_SIZE:(i + 1) * self.BATCH_SIZE], ordered=False)
                    #10 sec

                else:
                    self.currentCollection.insert_many(documents[i * self.BATCH_SIZE:], ordered=False)

            except pymongo.errors.BulkWriteError as error:
                # duplicateCount += 1

                # print(error)
                pass

        print("Duplicate count = ", duplicateCount)

    def insertAll(self, documents):

        try:
            self.currentCollection.insert_many(documents, ordered=False)
        except pymongo.errors.BulkWriteError as error:
            # print(error)
            pass

    # Get config data from the config file
    def getConfigData(self):

        configLocation = "config\config.json"
        configFile = open(configLocation, "r")
        configData = configFile.read()
        return configData

    # Get wanted info from config data
    def getInfo(self, configKey):

        data = json.loads(self.configData)
        field = str(data[configKey])

        return field

    # Terminate DB connection
    def close(self):

        self.client.close()
