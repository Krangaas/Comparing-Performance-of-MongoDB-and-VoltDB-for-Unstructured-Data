import os, subprocess
import time
from pop_mongoDB import *
from statistics import mean

N_FILES = [1, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
iterations = 10
avg_write = []
avg_read = []
avg_delete = []

def single_op(iterations=10):
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
    avg_write = []
    avg_read = []
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
    print("avg_read : ", avg_read)
    print("avg_del  : ", avg_delete)

if __name__=='__main__':
    try:
        cmd = sys.argv[1]
    except:
        print("To Use: 'python3 testMongo.py ARG'")
        print("Valid inputs are: [singe, multi]")
        exit(0)

    if cmd == 'single':
        single_op()
    elif cmd == 'multi':
        multi_op()
    else:
        print("To Use: 'python3 testMongo.py ARG'")
        print("Valid inputs are: [singe, multi]")
        exit(0)
