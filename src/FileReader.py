import json

# Working code with the assumption that the input has "UTF-8" encoding
def test():

    fileName = r"C:\Users\efe.yukselen\PycharmProjects\datainjection\data\requests\2015\09\30\zed-log\07-requests.json"
    testFile = open(fileName, "r")
    contents = testFile.read()
    print(contents)
    testFile.close()
    return

test()