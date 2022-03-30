import os, subprocess
from mongoDB_utils import *
from statistics import mean

N_FILES = [1, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]

def single_op(iterations=10):
    """ Store, select and delete N cat images with single-operations. """
    avg_write = []
    avg_read = []
    avg_delete = []

    for N in N_FILES:
        print("N_FILES", N)
        write_times = []
        read_times = []
        delete_times = []
        for i in range(iterations):
            print("     iteration", i)
            write_times.append(mdb_insert(N))
            read_times.append(mdb_select(N, do_show=False))
            delete_times.append(mdb_delete(N))
        avg_write.append(mean(write_times))
        avg_read.append(mean(read_times))
        avg_delete.append(mean(delete_times))

    print("avg_write: ", avg_write)
    print("avg_read : ", avg_read)
    print("avg_del  : ", avg_delete)


def multi_op(iterations=3):
    """ Store and delete N cat images with multi-operations."""
    avg_write = []
    #avg_read = []
    avg_delete = []

    for N in N_FILES:
        write_times = []
        read_times = []
        delete_times = []
        for i in range(iterations):
            print("     iteration", i)
            write_times.append(mdb_insert(N))
            #read_times.append(mdb_multiselect())
            delete_times.append(mdb_multidelete())
        avg_write.append(mean(write_times))
        #avg_read.append(mean(read_times))
        avg_delete.append(mean(delete_times))

    print("avg_write: ", avg_write)
    #print("avg_read : ", avg_read)
    print("avg_del  : ", avg_delete)


def show_cats(N):
    """ Store, select, show and delete N cat images. """
    N = int(N)
    mdb_insert(N)
    mdb_select(N, do_show=True)
    mdb_delete(N)


def help_and_exit():
    """ Display help text and exit. """
    print("To Benchmark: 'python3 test_mongoDB.py [singe, multi]'")
    print("To Show N cat images: 'python3 test_mongoDB.py cats N'")
    exit(0)


if __name__=='__main__':
    try:
        cmd = sys.argv[1]
    except:
        help_and_exit()

    if cmd == "single":
        single_op()
    elif cmd == "multi":
        multi_op()
    elif cmd == "cats":
        try:
            cmd2 = sys.argv[2]
        except:
            help_and_exit()
        show_cats(cmd2)
    else:
        help_and_exit()
