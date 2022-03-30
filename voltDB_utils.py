from importlib.resources import path
from matplotlib.pyplot import close
from numpy import size
from voltdbclient import *
import io
import matplotlib.pyplot as plt
from PIL import Image
import time
import os
import subprocess

path = './catfolder/'
cell_limit = 1000000
row_limit = 2097000

def vdb_insert(N_FILES):
    """ Insert N files into the database, one at a time.
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
    return tot_time


def vdb_select(N_FILES, do_show=False):
    """
    Select N files from the database, one at a time.
    Enable do_show to display the images.
    """
    client = FastSerializer("localhost", 21212)
    proc = VoltProcedure( client, "Select", [FastSerializer.VOLTTYPE_STRING])

    FETCHED_FILES = 0
    t1 = time.time()
    for filename in os.listdir(path):
        if FETCHED_FILES < N_FILES:
            response = proc.call([filename])
            FETCHED_FILES += 1
            if do_show:
                processed_img = postprocess(response.tables[0].tuples[0])
                plot_img(processed_img)
    t2 = time.time()
    tot_time = t2-t1
    return tot_time


def vdb_multiselect():
    """ Select multiple files from the database at the same time. """
    t1 = time.time()
    os.system("sqlcmd --query='SELECT * FROM CAT;' > /dev/null")
    t2 = time.time()
    tot_time = t2-t1
    return tot_time


def vdb_delete(N_FILES):
    """ Delete N files from the database, one at a time. """
    client = FastSerializer("localhost", 21212)
    proc = VoltProcedure( client, "Delete", [FastSerializer.VOLTTYPE_STRING])

    DELETED_FILES = 0
    t1 = time.time()
    for filename in os.listdir(path):
        if DELETED_FILES < N_FILES:
            response = proc.call([filename])
            DELETED_FILES += 1
    t2 = time.time()
    tot_time = t2-t1
    return tot_time


def vdb_multidelete():
    """ Delete multiple files from the database at the same time. """
    t1 = time.time()
    os.system("sqlcmd --query='DELETE FROM CAT;' > /dev/null")
    t2 = time.time()
    tot_time = t2-t1
    return tot_time


def plot_img(image):
    """ Plot the input image. """
    pil_img = Image.open(io.BytesIO(image))
    plt.imshow(pil_img)
    plt.show()


def postprocess(data):
    """ Converts a string-converted and partitioned image into binary. """
    data = [x for x in data[1:] if x is not None]
    data = ''.join(data)
    data = bytes.fromhex(data)
    return data
