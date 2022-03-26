#!/usr/bin/env python3
import sys
from pymongo import MongoClient
import os

def run():
    connection = MongoClient("localhost", 27017)
    db = connection['CAT']
    images = db.images

    N_FILES = 1000
    PROCESSED_FILES = 0
    voltdb_size_limit = 2097000
    path = './catfolder/'

    for filename in os.listdir(path):
        if filename.endswith("jpg") and PROCESSED_FILES < N_FILES:
            file = path + filename
            with open(file, 'rb') as f:
                contents = f.read()
                str_contents = str(contents)
                file_size = sys.getsizeof(str_contents)
                if file_size < voltdb_size_limit:
                    image = {"filename":filename, 'images':contents}
                    images.insert_one(image).inserted_id
                    PROCESSED_FILES += 1


if __name__=='__main__':
    run()