import json
import os as os
import src.utils as utils


class FileReader:

    def __init__(self):

        self.config_data = utils.get_config_data()
        self.root = utils.get_info(self.config_data, "JSON_PATH")
        self.backup_folder = utils.get_info(self.config_data, "BACKUP_FOLDER")

        self.files = []
        self.documents = []

    # Get the .json files to be read in a list
    def get_files(self):

        # print("Backup folder: ", self.backup_folder)

        for current_path, directory, files in os.walk(self.root):

            for current_filename in files:

                if '.json' in current_filename and not current_filename.startswith('._') and self.backup_folder not in current_path:

                    self.files.append(os.path.join(current_path, current_filename))

        return self.files

    # Read the json file line by line and return the dictionaries as a list
    def read_JSON(self, file_name):

        json_dicts = []
        current_file = open(file_name, "r")
        contents = current_file.read()

        try:
            lines = contents.splitlines()

            for line in lines:
                data = json.loads(line)
                data["file_location"] = file_name
                # print(data)
                json_dicts.append(data)

        except ValueError:

            print("Value Error! There is a problem with the json file! ")
            # pass

        current_file.close()
        return json_dicts

    def prepare_documents(self, json_dicts):

        self.documents = []

        for current_dict in json_dicts:
            for document in current_dict:

                removed_keys = []
                added_attributes = []

                for key, attribute in document.items():
                    if type(attribute) is dict:
                        added_attributes.append(attribute)
                        removed_keys.append(key)

                    # elif key == "user_agent":
                        # agents = attribute.split(")")
                        # document[key] = agents

                for key in removed_keys:
                    document.pop(key)

                for attribute in added_attributes:
                    document.update(attribute)

                self.documents.append(document)

        return self.documents

    # Count the number of .json files in facebook-backup
    def count_backup(self):

        backup_docs = []

        file_locations = [f for f in self.root.glob(self.backup_folder + "/**/*") if
                          f.is_file() and not f.name.startswith("._") and f.name.endswith(".json")]

        for current_file in file_locations:

            json_dicts = self.read_JSON(current_file)
            for dict in json_dicts:
                backup_docs.append(dict)

        backup_size = backup_docs.__len__()
        print("Backup size = ", backup_size)

        return backup_docs

    # Test
    def test(self):

        print("Config data = ", self.config_data)
        print("Root = ", self.root)

        self.get_files()
        print(self.files)

        json_dicts = self.read_JSON(self.files[0])
        for dict in json_dicts:
            self.documents.append(dict)

        print(self.documents)

        return






