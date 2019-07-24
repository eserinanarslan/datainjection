from pymongo import MongoClient
import pymongo.errors
import pymongo
import time
import src.utils as utils


class DBMS:

    def __init__(self, collection_name):

        self.collection_name = collection_name
        self.config_data = utils.get_config_data()

        self.client = MongoClient(utils.get_info(self.config_data, "CLIENT_URL"),
                                  username=utils.get_info(self.config_data, "USER_NAME"),
                                  password=utils.get_info(self.config_data, "PASSWORD"),)

        self.db = self.client[utils.get_info(self.config_data, "DB_NAME")]

        self.collections = self.db.collection_names()

        self.current_collection = self.db[self.collection_name]

        self.BATCH_SIZE = int(utils.get_info(self.config_data, "BATCH_SIZE"))
        self.TIME_COMMIT = int(utils.get_info(self.config_data, "TIME_COMMIT"))

    # Insert document to the current collection
    def insert_document(self, document):

        is_duplicate = False

        try:
            self.current_collection.insert_one(document)

        except pymongo.errors.DuplicateKeyError:
            is_duplicate = True

        return is_duplicate

    # Insert a list of documents unordered
    def insert_documents(self, documents):

        starting_time = time.time()

        # TODO add time, commit
        for i in range(0, (documents.__len__() // self.BATCH_SIZE) + 1):

            try:
                if (i + 1) * self.BATCH_SIZE <= documents.__len__():
                    self.current_collection.insert_many(documents[i * self.BATCH_SIZE:(i + 1) * self.BATCH_SIZE], ordered=False)
                    #10 sec

                else:
                    self.current_collection.insert_many(documents[i * self.BATCH_SIZE:], ordered=False)

            except pymongo.errors.BulkWriteError as error:
                # print(error)
                pass

    def insert_all(self, documents):

        try:
            self.current_collection.insert_many(documents, ordered=False)
        except pymongo.errors.BulkWriteError as error:
            # print(error)
            pass

    # Insert documents one by one
    # Lasts approx 12 minutes
    def iteration_inject(self, documents):

        for document in documents:
            self.insert_document(document)

        return

    # Terminate DB connection
    def close(self):

        self.client.close()
