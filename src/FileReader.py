import json
from pathlib import Path
import os as os


class FileReader:

    def __init__(self) :

        self.configData = self.getConfigData()
        self.root = self.getRoot()

        self.files = []
        self.documents = []

    # Get config data from the config file
    def getConfigData(self):

        configLocation = "../config\config.json"
        configFile = open(configLocation, "r")
        configData = configFile.read()
        return configData

    # Get root path from the config data as relative path
    def getRoot(self):

        data = json.loads(self.configData)

        rootLocation = str(data["JSON_PATH"])

        return Path(rootLocation)

    # Get the .json files to be read in a list
    def getFiles(self):

        tempFiles = []

        backupFolder = self.getInfo("BACKUP_FOLDER")
        print("Backup folder: ", backupFolder)

        for currentPath, d, f in os.walk(self.root):
            for currentFileName in f:

                if '.json' in currentFileName and not currentFileName.startswith('._') and backupFolder not in currentPath:
                    tempFiles.append(os.path.join(currentPath, currentFileName))

        return tempFiles

    # Iterate through every "UTF-8" .json file in the folder
    # Write their relative paths to a list
    # Print the paths to the console
    def iterateFiles(self, files):

        for file in files:
            jsonDicts = self.readJSON(file)
            for dict in jsonDicts:
                self.documents.append(dict)

        return

    # Read the json file line by line and return the dictionaries as a list
    def readJSON(self, fileName):

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
    def getInfo(self, configKey):

        data = json.loads(self.configData)
        field = str(data[configKey])

        return field

    def setDocuments(self, documents):

        self.documents = documents
        return

    # Test methods
    def test(self):

        print("Config data = ", self.configData)
        print("Root = ", self.root)

        self.getFiles()
        print(self.files)

        jsonDicts = self.readJSON(self.files[0])
        for dict in jsonDicts:
            self.documents.append(dict)

        print(self.documents)

        return


# fileReader = FileReader()
# fileReader.test()





