import json
from pathlib import Path
import os as os
import time
from src.DBMS import DBMS as DBMS
from src.FileReader import FileReader as FileReader
import multiprocessing as mp
import multiprocessing.pool as pool


def main():

    start = time.time()

    fileReader = FileReader()

    # Create processes equal to the number of cpu's
    processCount = mp.cpu_count()
    print("The number of threads is: ", processCount)
    processes = pool.Pool(processCount)

    initTime = time.time()
    print("Pre file reading duration is: " , initTime - start)

    # Add files to be read to a list
    fileReader.getFiles()
    files = fileReader.files

    # Iterate through every file in the folder
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
    DB = DBMS(collectionName)

    duplicateCount = 0
    for document in documents:
        if DB.insertDocument(document):  # If the document is duplicate
            duplicateCount += 1

    print("Duplicate count = ", duplicateCount)

    injectionTime = time.time()
    print("Database injection time is: ", injectionTime - readTime)

    DB.close()

    return


if __name__ == '__main__':

    main()
