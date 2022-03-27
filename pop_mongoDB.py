#!/usr/bin/env python3
import sys
from pymongo import MongoClient
import os
import io
import matplotlib.pyplot as plt
from PIL import Image
import time


N_FILES = int(sys.argv[1])

def insert():
    connection = MongoClient("localhost", 27017)
    db = connection['CAT']
    images = db.images
    path = './catfolder/'

    PROCESSED_FILES = 0
    t1 = time.time()
    for filename in os.listdir(path):
        if filename.endswith("jpg") and PROCESSED_FILES < N_FILES:
            file = path + filename
            with open(file, 'rb') as f:
                contents = f.read()
                image = {"filename":filename, 'images':contents}
                images.insert_one(image).inserted_id
                PROCESSED_FILES += 1
    t2 = time.time()
    tot_time = t2 - t1
    print(tot_time)

def select():
    connection = MongoClient("localhost", 27017)
    db = connection['CAT']
    images = db.images
    t1 = time.time()
    response = images.find()
    t2 = time.time()
    tot_time = t2 - t1
    print(tot_time)



def plot_img(data):
    pil_image = Image.open(io.BytesIO(data["images"]))
    plt.imshow(pil_image)
    plt.show()



if __name__=='__main__':
    insert()
    #select()