import json
from pathlib import Path
import os as os
import time
from src.DBMS import DBMS as DBMS
import threading
import multiprocessing


def main():

    start = time.time()

    # threadCount = multiprocessing.cpu_count()
    # print("The number of threads is: ", threadCount)

    # Get config data
    configData = getConfigData()

    # Get the root from the config file
    rootLocation, root = getRoot(configData)

    initTime = time.time()
    print("Pre file reading duration is: " , initTime - start)

    # Iterate through every file in the folder
    files, documents = iterateFiles(root, configData)

    backupDocs = []
    fileLocations = [f for f in root.glob("facebook-backup/**/*") if
                     f.is_file() and not f.name.startswith("._") and f.name.endswith(".json")]
    for currentFile in fileLocations:
        jsonDicts = readJSON(currentFile)
        for dict in jsonDicts:
            backupDocs.append(dict)

    backupSize = backupDocs.__len__()
    print("Backup size = ", backupSize)

    readTime = time.time()
    print("File reading duration is: ", readTime - initTime)

    # Connect to db
    DB = DBMS("Requests")

    duplicateCount = 0
    for document in documents:
        if DB.insertDocument(document):  # If the document is duplicate
            duplicateCount += 1

    print("Duplicate count = ", duplicateCount)

    injectionTime = time.time()
    print("Database injection time is: ", injectionTime - readTime)
    
    # End connection with db

    return


# Get config data from the config file
def getConfigData():

    configLocation = "../config\config.json"
    configFile = open(configLocation, "r")
    configData = configFile.read()
    return configData


# Get root path from the config data as relative path
def getRoot(configData):

    data = json.loads(configData)

    rootLocation = str(data["JSON_PATH"])

    return rootLocation, Path(rootLocation)


# Iterate through every "UTF-8" .json file in the folder
# Write their relative paths to a list
# Print the paths to the console
def iterateFiles(root, configData):

    documents = []  # All the individual documents to be added to the database
    docCounter = 0  # Counts the times a document is added to documents

    files = []  # All the .json files to be read
    fileCounter = 0  # Counts the times a file is read

    backupFolder = getInfo(configData, "BACKUP_FOLDER")
    print("Backup folder: ", backupFolder)

    for currentPath, d, f in os.walk(root):
        for currentFileName in f:

            if '.json' in currentFileName and not currentFileName.startswith('._') and backupFolder not in currentPath:
                files.append(os.path.join(currentPath, currentFileName))
                fileCounter += 1

                jsonDicts = readJSON(currentPath + "\\" + currentFileName)
                for dict in jsonDicts:
                    documents.append(dict)
                    docCounter += 1

    print("File counter =", fileCounter)
    print("File List size = ", files.__len__())
    print("Doc counter = ", docCounter)
    print("Doc List size = ", documents.__len__())

    return files, documents


# Read the json file line by line and return the dictionaries as a list
def readJSON(fileName):

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

    currentFile.close()
    return jsonDicts


# Get wanted info from config data
def getInfo(configData, configKey):

    data = json.loads(configData)
    field = str(data[configKey])

    return field


main()




