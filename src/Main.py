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
    processCount = mp.cpu_count()
    print("The number of threads is: ", processCount)

    initTime = time.time()
    print("Pre file reading duration is: ", initTime - start)

    # Add files to be read to a list
    files = fileReader.getFiles()
    print(files)
    print(files[0])

    # fileReader.iterateFiles(files)
    # documents = fileReader.documents

    # Iterate through every file in the folder
    processes = pool.Pool(processCount)
    processes.map(fileReader.iterateFiles, files)
    documents = fileReader.documents

    processes.close()


    """backupDocs = []
    fileLocations = [f for f in root.glob("facebook-backup/**/*") if
                     f.is_file() and not f.name.startswith("._") and f.name.endswith(".json")]
    for currentFile in fileLocations:
        jsonDicts = readJSON(currentFile)
        for dict in jsonDicts:
            backupDocs.append(dict)

    backupSize = backupDocs.__len__()
    print("Backup size = ", backupSize)
    """

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


def iterateFiles(files):

    documents = []
    for file in files:
        jsonDicts = readJSON(file)
        for dict in jsonDicts:
            documents.append(dict)

    return documents


def readJSON(fileName):

    print("Current file: ", fileName)
    currentFile = open(fileName, "r")
    contents = currentFile.read()
    jsonDicts = []

    try:
        lines = contents.splitlines()
        for line in lines:
            data = json.loads(line)
            # print(data)
            jsonDicts.append(data)

    except ValueError:

        print("Value Error! There is a problem with the json file! ")
        # pass

    print("File done!")
    currentFile.close()
    return jsonDicts


if __name__ == '__main__':

    main()
