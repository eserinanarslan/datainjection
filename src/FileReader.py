import json
from pathlib import Path
from pymongo import MongoClient
import time
import src.DBMS as DBMS


def main():

    start = time.time()

    # Get config data
    configData = getConfigData()

    # Get the root from the config file
    root = getRoot(configData)

    # Connect to db
    # DB = DBMS("requests")

    end1 = time.time()
    print("Pre file reading duration is: " , end1 - start)

    # Iterate through every file in the folder
    documents, fileCount, docCount = iterateFiles(root)
    print("File count is: ", fileCount, "Document count is: ", docCount)

    end2 = time.time()
    print("File reading duration is: ", end2 - end1)


    # Upload printed data to the db
    # End connection with db

    # outputFile.close()

    return


# Get config data from the config file
def getConfigData():

    configLocation = "../config\config.json"
    configFile = open(configLocation, "r")
    configData = configFile.read()
    # print(configData)
    return configData


# Get root path from the config data as relative path
def getRoot(configData):

    data = json.loads(configData)
    # print(data)

    rootLocation = str(data["JSON_PATH"])
    # print("Root location is: " + rootLocation)

    return Path(rootLocation)


# Iterate through every "UTF-8" .json file in the folder
# Write their relative paths to a list
# Print the paths to the console
def iterateFiles(root):

    fileCount = 0  # Number of UTF-8 json files in the folder

    # Create a list for all json files in the folder
    # Not including the non "UTF-8" files
    fileLocations = [f for f in root.glob('**/*.json') if f.is_file() and not f.name.startswith("._")]
    fileCount = fileLocations.__sizeof__()

    documents = []  # All the individual documents to be added to the database
    docCount2 = 0
    for currentFileLocation in fileLocations:
        # print(currentFileLocation)
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
    # print(contents)
    jsonDicts = []

    try:
        lines = contents.splitlines()
        for line in lines:
            data = json.loads(line)
            # print(data)
            jsonDicts.append(data)

    except ValueError:
        print("Value Error! There is a problem with the json file! ")

    currentFile.close()
    # print(jsonDicts)
    return jsonDicts


def createDB():

    client = MongoClient()
    DB = client["DataInJectionDB"]

    collection1 = DB["Requests"]

    return client, DB, collection1


# Test if an arbitrarily selected file can be read
def test():

    fileName = r"..\data\requests\facebook-backup\2015\11\24\zed-log\10-requests.json"
    testFile = open(fileName, "r")
    contents = testFile.read()
    #print(contents)
    try:
        lines = contents.splitlines()
        for line in lines:
            data = json.loads(line)
            print(data)

    except ValueError:
        print("lmao")

    testFile.close()
    return

# test()
main()
# getRoot(getConfigData())
# readJSON(r"..\data\requests\facebook-backup\2015\11\24\zed-log\10-requests.json")

