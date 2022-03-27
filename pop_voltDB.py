from importlib.resources import path
from matplotlib.pyplot import close
from numpy import size
from voltdbclient import *
import io
import matplotlib.pyplot as plt
from PIL import Image
import time
import os


path = './catfolder/'
#N_FILES = int(sys.argv[1])

def vdb_insert(N_FILES):
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
    return tot_time


def vdb_select(N_FILES):
    client = FastSerializer("localhost", 21212)
    proc = VoltProcedure( client, "Select", [FastSerializer.VOLTTYPE_STRING])

    fetched_files = 0
    t1 = time.time()
    for filename in os.listdir(path):
        if fetched_files < N_FILES:
            response = proc.call([filename])
            fetched_files += 1
    t2 = time.time()
    tot_time = t2-t1
    return tot_time


def vdb_multiselect():
    t1 = time.time()
    os.system("sqlcmd --query='SELECT * FROM CAT;' > /dev/null")
    t2 = time.time()
    tot_time = t2-t1
    return tot_time

def vdb_delete(N_FILES):
    client = FastSerializer("localhost", 21212)
    proc = VoltProcedure( client, "Delete", [FastSerializer.VOLTTYPE_STRING])

    deleted_files = 0
    t1 = time.time()
    for filename in os.listdir(path):
        if deleted_files < N_FILES:
            response = proc.call([filename])
    t2 = time.time()
    tot_time = t2-t1
    return tot_time



def vdb_multidelete():
    t1 = time.time()
    os.system("sqlcmd --query='DELETE FROM CAT;' > /dev/null")
    t2 = time.time()
    tot_time = t2-t1
    return tot_time




def plot_img(data):
    cat_pic = bytes.fromhex(data.tables[0].tuples[0][1])
    pil_img = Image.open(io.BytesIO(cat_pic))
    plt.imshow(pil_img)
    plt.show()

#if __name__=='__main__':
#    vdb_insert(3)
