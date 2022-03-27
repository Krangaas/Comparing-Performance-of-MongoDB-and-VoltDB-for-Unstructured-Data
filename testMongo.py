import os, subprocess
import time
from pop_mongoDB import *
from statistics import mean

N_FILES = [1, 1000, 2000]#, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
tries = 10
avg_write = []
avg_read = []
avg_delete = []



for N in N_FILES:
    print("N_FILES", N)
    write_times = []
    read_times = []
    delete_times = []
    for i in range(tries):
        print("     iteration", i)
        write_times.append(mdb_insert(N))
        read_times.append(mdb_select(N))
        delete_times.append(mdb_delete(N))
    avg_write.append(mean(write_times))
    avg_read.append(mean(read_times))
    avg_delete.append(mean(delete_times))


# ToDo: create loop for multi operations.