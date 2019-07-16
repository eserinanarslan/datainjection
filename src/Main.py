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

    for dict in jsonDicts:
        for document in dict:
            documents.append(document)

    processes.close()

    readTime = time.time()
    print("File reading duration is: ", readTime - initTime)

    # Connect to db
    collectionName = fileReader.getInfo("COLLECTION_NAME")
    DB = dbms(collectionName)

    """
    duplicateCount = 0
    duplicates = []
    for document in documents:
        if DB.insertDocument(document):  # If the document is duplicate
            duplicateCount += 1
            print("Document = ", document)
            duplicates.append(document)
            duplicates.append(DB.currentCollection.find({"request_id": document["request_id"]}))
            print("DB = ", DB.currentCollection.find_one({"request_id": document["request_id"]}))

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
    result = db[collectionName].map_reduce(map, reduce, "myresults")
    print(result.distinct('_id'))
    return result.distinct('_id')


if __name__ == '__main__':

    main()
    # get_keys()
