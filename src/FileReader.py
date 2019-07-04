import json
from pathlib import Path
import pymongo


def main():

    # Open a local file to print data?
    # outputFile = open("outputFile.txt", "w+")

    # Get config data
    configData = getConfigData()

    # Get the root from the config file
    root = getRoot(configData)

    # Iterate through every file in the folder
    fileCount = iterateFiles(root)
    print("File count is: " + fileCount)

    # Connect to db
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

    rootLocation = "../"
    rootLocation += str(data["JSON_PATH"])
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

    # Print the names of all json files in the folder
    for currentFileLocation in fileLocations:
        print(currentFileLocation)

        # Print data to output file
        # currentFile = open(currentFileLocation, "r")
        # contents = currentFile.read()
        # outputFile.write(contents)
        # currentFile.close()

    return fileCount


# Test if an arbitrarily selected file can be read
def test():

    fileName = r"..\data\requests\facebook-backup\2015\11\24\zed-log\10-requests.json"
    testFile = open(fileName, "r")
    contents = testFile.read()
    print(contents)
    testFile.close()
    return

# test()
main()