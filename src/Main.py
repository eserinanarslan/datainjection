import json
from pathlib import Path
import os as os
import time

from src.DBMS import DBMS as dbms
from src.FileReader import FileReader as fr
import multiprocessing as mp
import multiprocessing.pool as pool


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

    # Add files to be read to a list
    # fileReader.getFiles()
    # files = fileReader.files
    # print(files)
    # print(files[0])

    # fileReader.iterateFiles(files)
    # documents = fileReader.documents

    # Iterate through every file in the folder
    processes = pool.Pool(processCount)

    fileReader.getFiles()
    files = fileReader.files

    documents = []
    jsonDicts = processes.map(fileReader.readJSON, files)

    for dict in jsonDicts:
        for document in dict:
            documents.append(document)

    print(" First element = ", documents[0])

    processes.close()

    readTime = time.time()
    print("File reading duration is: ", readTime - initTime)

    # Connect to db
    collectionName = fileReader.getInfo("COLLECTION_NAME")
    DB = dbms(collectionName)

    duplicateCount = 0
    for document in documents:
        if DB.insertDocument(document):  # If the document is duplicate
            duplicateCount += 1

    print("Duplicate count = ", duplicateCount)

    injectionTime = time.time()
    print("Database injection time is: ", injectionTime - readTime)

    DB.close()

    return


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


if __name__ == '__main__':

    main()
