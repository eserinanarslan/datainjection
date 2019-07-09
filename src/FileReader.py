import json
from pathlib import Path
from pymongo import MongoClient
import time
from src.DBMS import DBMS as DBMS
import threading
import multiprocessing

def main():

    start = time.time()

    threadCount = multiprocessing.cpu_count()
    print("The number of threads is: ", threadCount)

    # Get config data
    configData = getConfigData()

    # Get the root from the config file
    root = getRoot(configData)

    # Connect to db
    DB = DBMS("Requests")

    end1 = time.time()
    print("Pre file reading duration is: " , end1 - start)

    # Iterate through every file in the folder
    documents, fileCount, docCount = iterateFiles(root)
    print("File count is: ", fileCount, "Document count is: ", docCount)

    end2 = time.time()
    print("File reading duration is: ", end2 - end1)

    for document in documents:
        DB.insertDocument(document)

    end3 = time.time()
    print("Database injection time is: ", end3 - end2)

    # Upload printed data to the db
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

    return Path(rootLocation)


# Iterate through every "UTF-8" .json file in the folder
# Write their relative paths to a list
# Print the paths to the console
def iterateFiles(root):

    fileCount = 0  # Number of UTF-8 json files in the folder

    # Create a list for all json files in the folder
    # Not including the non "UTF-8" files
    fileLocations = []
    folders = root.glob('./!facebook-backup*')
    fileLocations = [f for f in root.glob('**/*.json') if f.is_file() and not f.name.startswith("._") and f not in root.glob('facebook-backup/**')]
    fileCount = fileLocations.__sizeof__()

    documents = []  # All the individual documents to be added to the database
    docCount2 = 0
    for currentFileLocation in fileLocations:

        jsonDicts = readJSON(currentFileLocation)
        for dict in jsonDicts:
            documents.append(dict)
            docCount2 += 1

    docCount = documents.__len__()
    print("Doc counter = ", docCount2)
    return documents, fileCount, docCount


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


