import time
from src.DBMS import DBMS as dbms
from src.FileReader import FileReader as fr
import multiprocessing as mp
import multiprocessing.pool as pool
from bson import Code
from pymongo import MongoClient


def main():

    start = time.time()

    fileReader = fr()

    # Create processes equal to the number of cpu's
    if mp.cpu_count() >= 2:
        processCount = mp.cpu_count() - 1

    else:
        processCount = 1

    print("The number of threads is: ", processCount)

    initTime = time.time()
    print("Pre file reading duration is: ", initTime - start)

    fileReader.getFiles()
    files = fileReader.files
    documents = []

    processes = pool.Pool(processCount)
    jsonDicts = processes.map(fileReader.readJSON, files)
    processes.close()
    counter = 0

    for currentDict in jsonDicts:
        for document in currentDict:
            removedKeys = []
            addedAttributes = []
            for key, attribute in document.items():
                if type(attribute) is dict:
                    counter += 1
                    addedAttributes.append(attribute)
                    removedKeys.append(key)

            for key in removedKeys:
                document.pop(key)
            for attribute in addedAttributes:
                document.update(attribute)

            documents.append(document)

    print("Counter = ", counter)

    readTime = time.time()
    print("File reading duration is: ", readTime - initTime)

    # Connect to db
    collectionName = fileReader.getInfo("COLLECTION_NAME")
    DB = dbms(collectionName)


    """
    duplicateCount = 0
    duplicates = []
    # isUnique = True
    # nonUnique = 0
    for document in documents:
        if DB.insertDocument(document):  # If the document is duplicate
            duplicateCount += 1
            duplicates.append(document)
            duplicates.append(DB.currentCollection.find({"request_id": document["request_id"]}))

            
            for item in document.items():
                if type(item) is dict:
                    for otherItem in DB.currentCollection.find_one({"request_id": document["request_id"]}).items():
                        if type(otherItem) is dict:
                            otherList = otherItem
                            break

                    if item != otherList:
                        isUnique = False

                else:
                    if item not in DB.currentCollection.find_one({"request_id": document["request_id"]}).items():
                        isUnique = False

            if not isUnique:
                print("Document = ", document)
                print("DB = ", DB.currentCollection.find_one({"request_id": document["request_id"]}))
                nonUnique += 1
            

    # print("The number of nonuniques = ", nonUnique)
    print("Duplicate count = ", duplicateCount)
    # print(duplicates)
    print("Duplicates Length = ", duplicates.__len__())
    """


    DB.insertDocuments(documents)

    injectionTime = time.time()
    print("Database injection time is: ", injectionTime - readTime)

    DB.close()

    return


# Count the number of .json files in facebook-backup
def countBackup(root):

    backupDocs = []
    fileReader = fr()
    fileLocations = [f for f in root.glob("facebook-backup/**/*") if
                     f.is_file() and not f.name.startswith("._") and f.name.endswith(".json")]
    for currentFile in fileLocations:
        jsonDicts = fileReader.readJSON(currentFile)
        for dict in jsonDicts:
            backupDocs.append(dict)

    backupSize = backupDocs.__len__()
    print("Backup size = ", backupSize)

    return


def get_keys():

    fileReader = fr()

    configData = fileReader.getConfigData()

    client = MongoClient(fileReader.getInfo("CLIENT_URL"))

    db = client[fileReader.getInfo("DB_NAME")]
    collectionName = fileReader.getInfo("COLLECTION_NAME")

    map = Code("function() { for (var key in this) { emit(key, null); } }")
    reduce = Code("function(key, stuff) { return null; }")
    result = db[collectionName].map_reduce(map, reduce, "Attributes")
    print(result.distinct('_id'))
    return result.distinct('_id')


if __name__ == '__main__':

    # main()
    get_keys()
