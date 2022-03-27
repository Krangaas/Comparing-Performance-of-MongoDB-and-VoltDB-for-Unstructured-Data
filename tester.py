import os
import time
import subprocess
from statistics import mean
from pop_voltDB import *

N_FILES = [1, 1000, 2000]#, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]
tries = 10
avg_write = []
avg_read = []
avg_delete = []

os.system("voltdb init --force > /dev/null")
subprocess.Popen(["voltdb", "start"], stdout=subprocess.DEVNULL)
time.sleep(5)
os.system("sqlcmd < CAT_SETUP_TABLE > /dev/null")

for N in N_FILES:
    print("N_FILES", N)
    write_times = []
    read_times = []
    delete_times = []
    for i in range(tries):
        print("     iteration", i)
        write_times.append(vdb_insert(N))
        read_times.append(vdb_select(N))
        delete_times.append(vdb_delete())
    avg_write.append(mean(write_times))
    avg_read.append(mean(read_times))
    avg_delete.append(mean(delete_times))

print(avg_write)
print(avg_read)
print(avg_delete)
subprocess.call(["voltadmin", "shutdown"], stdout=subprocess.DEVNULL)
