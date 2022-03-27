#!/usr/bin/env python3
import sys
from pymongo import MongoClient
import os
import io
import matplotlib.pyplot as plt
from PIL import Image


N_FILES = int(sys.argv[1])

def insert():
    connection = MongoClient("localhost", 27017)
    db = connection['CAT']
    images = db.images
    path = './catfolder/'

    PROCESSED_FILES = 0
    for filename in os.listdir(path):
        if filename.endswith("jpg") and PROCESSED_FILES < N_FILES:
            file = path + filename
            with open(file, 'rb') as f:
                contents = f.read()
                image = {"filename":filename, 'images':contents}
                images.insert_one(image).inserted_id
                PROCESSED_FILES += 1

def select():
    connection = MongoClient("localhost", 27017)
    db = connection['CAT']
    images = db.images
    response = images.find_one()
    print(response)
    pil_image = Image.open(io.BytesIO(response["images"]))
    plt.imshow(pil_image)
    plt.show()

    #for x in response:
    #    print(x)


def plot_img(data):
    pil_image = Image.open(io.BytesIO(data["images"]))
    plt.imshow(pil_image)
    plt.show()



if __name__=='__main__':
    #insert()
    select()