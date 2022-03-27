import os
import time
import subprocess
from statistics import mean
from pop_voltDB import *

N_FILES = [1, 1000, 2000]#, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]

def single_op(tries=10):
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
            delete_times.append(vdb_delete(N))
        avg_write.append(mean(write_times))
        avg_read.append(mean(read_times))
        avg_delete.append(mean(delete_times))

    subprocess.call(["voltadmin", "shutdown"], stdout=subprocess.DEVNULL)
    print(avg_write)
    print(avg_read)
    print(avg_delete)


def multi_op(tries=10):
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
            read_times.append(vdb_multiselect())
            delete_times.append(vdb_multidelete())
        avg_write.append(mean(write_times))
        avg_read.append(mean(read_times))
        avg_delete.append(mean(delete_times))

    subprocess.call(["voltadmin", "shutdown"], stdout=subprocess.DEVNULL)
    print(avg_write)
    print(avg_read)
    print(avg_delete)


if __name__=='__main__':
    try:
        cmd = sys.argv[1]
    except:
        print("To Use: 'python3 tester.py ARG'")
        print("Valid inputs are: [singe, multi]")
        exit(0)

    if cmd == "single":
        single_op()
    elif cmd == "multi":
        multi_op()
    else:
        print("To Use: 'python3 tester.py ARG'")
        print("Valid inputs are: [singe, multi]")
        exit(0)
