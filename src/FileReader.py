import json
from pathlib import Path
import os as os


class FileReader:

    def __init__(self):

        self.configData = self.getConfigData()
        self.root = self.getRoot()

        self.files = []
        self.documents = []

        # print("FileReader created!")
        # print("Root = ", self.root)

    # Get config data from the config file
    def getConfigData(self):

        configLocation = "config/config.json"
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

        backupFolder = self.getInfo("BACKUP_FOLDER")
        # print("Backup folder: ", backupFolder)

        for currentPath, directory, files in os.walk(self.root):

            for currentFileName in files:

                if '.json' in currentFileName and not currentFileName.startswith('._') and backupFolder not in currentPath:

                    self.files.append(os.path.join(currentPath, currentFileName))

        return

    def addDocument(self, document):

        self.documents.append(document)
        return

    # Read the json file line by line and return the dictionaries as a list
    def readJSON(self, fileName):

        jsonDicts = []
        currentFile = open(fileName, "r")
        # print(fileName)
        # print(currentFile)
        contents = currentFile.read()

        try:
            lines = contents.splitlines()
            for line in lines:
                data = json.loads(line)
                data["file_location"] = fileName
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

    def prepareDocuments(self, jsonDicts):
        documents = []
        counter = 0

        for currentDict in jsonDicts:
            for document in currentDict:
                removedKeys = []
                addedAttributes = []
                for key, attribute in document.items():
                    if type(attribute) is dict:
                        counter += 1
                        addedAttributes.append(attribute)
                        removedKeys.append(key)

                for key in removedKeys:
                    document.pop(key)
                for attribute in addedAttributes:
                    document.update(attribute)

                documents.append(document)

        # print("Document Counter = ", counter)
        return documents

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






