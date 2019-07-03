import json
from pathlib import Path
import os


def main():

    # Open a local file to print data?

    # Get config data
    configData = getConfigData()

    # Get the root from the config file
    root = getRoot()

    # Create a list for all json files in the folder
    fileLocations = [f for f in root.glob('**/*.json') if f.is_file()]

    # Print the names of all json files in the folder
    for currentFileLocation in fileLocations:
        print(currentFileLocation)

        # Print data to output file

    # Connect to db
    # Upload printed data to the db
    # End connection with db

    return

# Get config data from the config file
def getConfigData():

    configLocation = "../config\config.json"
    configFile = open(configLocation, "r")
    configData = configFile.read()
    # print(configData)
    return configData


# Get root path from the config data as relative path
def getRoot():

    configData = getConfigData()

    data = json.loads(configData)

    rootLocation = "../"
    rootLocation += str(data["JSON_PATH"])
    # print("Root location is: " + rootLocation)

    return Path(rootLocation)


# Test if an arbitrarily selected file can be read
def test():

    fileName = r"..\data\requests\facebook-backup\2015\11\24\zed-log\10-requests.json"
    testFile = open(fileName, "r")
    contents = testFile.read()
    print(contents)
    testFile.close()
    return

# getRoot()
# test()
main()