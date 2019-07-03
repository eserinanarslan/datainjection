import json
from pathlib import Path
import os

# Working code with the assumption that the input has "UTF-8" encoding


def main():

    # Get config data
    # Establish root
    # Connect to db
    # Open a local file to print data
    # Iterate through root folders
    # Print data to output file
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


def test():

    # Get config data
    # configData = getConfigData()

    # Get the root from the config file
    root = getRoot()

    # Create a list for all the files in the folder
    files = [f for f in root.glob('**/*') if f.is_file()]

    # Print the names of all files in the folder
    for currentFile in files:
        # contents = currentFile.read()
        # print(contents)
        print(currentFile)


    # fileName = r"C:\Users\efe.yukselen\PycharmProjects\datainjection\data\requests\2015\09\30\zed-log\07-requests.json"
    # testFile = open(fileName, "r")
    # contents = testFile.read()
    # print(contents)
    # testFile.close()
    return

test()
# getRoot()