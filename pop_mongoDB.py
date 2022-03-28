#!/usr/bin/env python3
import sys
from pymongo import MongoClient
import os
import io
import matplotlib.pyplot as plt
from PIL import Image
import time

PATH = './catfolder/'

def mdb_insert(N_FILES):

    connection = MongoClient("localhost", 27017)
    db = connection['CAT']
    images = db.images
    PROCESSED_FILES = 0
    t1 = time.time()
    for filename in os.listdir(PATH):
        if filename.endswith("jpg") and PROCESSED_FILES < N_FILES:
            file = PATH + filename
            with open(file, 'rb') as f:
                contents = f.read()
                image = {"filename":filename, 'images':contents}
                images.insert_one(image).inserted_id
                PROCESSED_FILES += 1
    t2 = time.time()
    tot_time = t2 - t1
    #connection.drop_database('CAT')
    return tot_time

def mdb_select(N_FILES, do_show = False):
    connection = MongoClient("localhost", 27017)
    db = connection['CAT']
    images = db.images

    selected_files = 0
    t1 = time.time()
    for filename in os.listdir(PATH):
        if selected_files < N_FILES:
            image={'filename':filename}
            response = images.find_one(image)
            selected_files += 1
            if do_show:
                plot_img(response)
    t2 = time.time()
    tot_time = t2 - t1
    return tot_time

def mdb_multiselect():
    connection = MongoClient("localhost", 27017)
    db = connection['CAT']
    images = db.images
    t1 = time.time()
    response = images.find()
    t2 = time.time()
    tot_time = t2 - t1
    return tot_time

def mdb_delete(N_FILES):
    connection = MongoClient("localhost", 27017)
    db = connection['CAT']
    images = db.images
    deleted_files = 0
    t1 = time.time()
    for filename in os.listdir(PATH):
        if deleted_files < N_FILES:
            image={'filename':filename}
            response = images.find_one_and_delete(image)
            deleted_files += 1
    t2 = time.time()
    tot_time = t2 - t1
    return tot_time

def mdb_multidelete():
    connection = MongoClient("localhost", 27017)
    t1 = time.time()
    connection.drop_database('CAT')
    t2 = time.time()
    tot_time = t2 - t1
    return tot_time

def plot_img(data):
    pil_image = Image.open(io.BytesIO(data["images"]))
    plt.imshow(pil_image)
    plt.show()

