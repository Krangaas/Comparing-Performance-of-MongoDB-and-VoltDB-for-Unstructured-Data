#!/usr/bin/env python3

# This file is part of VoltDB.
# Copyright (C) 2008-2021 VoltDB Inc.
#
# Permission is hereby granted, free of charge, to any person obtaining
# a copy of this software and associated documentation files (the
# "Software"), to deal in the Software without restriction, including
# without limitation the rights to use, copy, modify, merge, publish,
# distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so, subject to
# the following conditions:
#
# The above copyright notice and this permission notice shall be
# included in all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
# EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
# MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.
# IN NO EVENT SHALL THE AUTHORS BE LIABLE FOR ANY CLAIM, DAMAGES OR
# OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE,
# ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR
# OTHER DEALINGS IN THE SOFTWARE.

from importlib.resources import path
from matplotlib.pyplot import close
from numpy import size
from voltdbclient import *
import io
import matplotlib.pyplot as plt
from PIL import Image
import time

N_FILES = int(sys.argv[1])

def insert():
    """ Insert all jpg files from specified folder into VoltDB.
        The method is customized to fit a Table Schema:
        TABLE CAT (
            FILENAME VARCHAR(25) NOT NULL,
            S0 VARCHAR(1000000 BYTES) NOT NULL,
            S1 VARCHAR(1000000 BYTES),
            S2 VARCHAR(97000 BYTES),
            PRIMARY KEY (FILENAME)
        );
    """

    client = FastSerializer("localhost", 21212)
    proc = VoltProcedure( client, "Insert", [FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING,
                                            FastSerializer.VOLTTYPE_STRING, FastSerializer.VOLTTYPE_STRING])

    path = './catfolder/'
    cell_limit = 1000000
    row_limit = 2097000
    PROCESSED_FILES = 0
    t1 = time.time()
    for filename in os.listdir(path):
        if filename.endswith("jpg") and PROCESSED_FILES < N_FILES:
            file = path + filename
            with open(file, 'rb') as f:
                contents = f.read()
                # convert binary data to hex
                str_contents = contents.hex()
                file_size = sys.getsizeof(str_contents)
                if file_size < row_limit:
                    s0 = str_contents[0:cell_limit]
                    # Write to next cell if the total filesize exceeds the cell limit of 1mil bytes.
                    if file_size > cell_limit:
                        s1 = str_contents[cell_limit:2*cell_limit]
                    else:
                        s1 = None
                    if file_size > 2*cell_limit:
                        s2 = str_contents[2*cell_limit:file_size]
                    else:
                        s2 = None
                    proc.call([filename, s0, s1, s2])
                    PROCESSED_FILES += 1

    t2 = time.time()
    tot_time = (t2-t1)
    print(tot_time)

def select():
    client = FastSerializer("localhost", 21212)
    proc = VoltProcedure( client, "Select", [FastSerializer.VOLTTYPE_STRING])
    path = './catfolder/'
    fetched_files = 0
    t1 = time.time()
    for filename in os.listdir(path):
        if fetched_files < N_FILES:
            response = proc.call([filename])
            fetched_files += 1
            #if fetched_files % 50 == 0:
            #    plot_img(response)
    t2 = time.time()
    tot_time = t2-t1
    print(tot_time)
    #print("Selected N files: ", fetched_files)

def plot_img(data):
    cat_pic = bytes.fromhex(data.tables[0].tuples[0][1])
    pil_img = Image.open(io.BytesIO(cat_pic))
    plt.imshow(pil_img)
    plt.show()

if __name__=='__main__':
    #insert()
    #select()
