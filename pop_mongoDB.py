from fileinput import filename
from pymongo import MongoClient
import os
import gridfs

def run():
    connection = MongoClient("localhost", 27017)
    db = connection['mongoDB']

    path = './archive/CAT_00/'
    fs = gridfs.GridFS(db)
    for filename in os.listdir("./archive/CAT_00/"):
        if filename.endswith("jpg"):
            file = path + filename
            print(file)
            with open(file, 'rb') as f:
                contents = f.read()
                fs.put(contents, filename="file")


if __name__=='__main__':
    run()