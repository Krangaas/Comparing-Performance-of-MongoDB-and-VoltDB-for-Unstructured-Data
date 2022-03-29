import os
import time
import subprocess
from statistics import mean
from pop_voltDB import *

N_FILES = [1, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]

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


def multi_op(tries=3):
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
            #read_times.append(vdb_multiselect())
            delete_times.append(vdb_multidelete())
        avg_write.append(mean(write_times))
        #avg_read.append(mean(read_times))
        avg_delete.append(mean(delete_times))

    subprocess.call(["voltadmin", "shutdown"], stdout=subprocess.DEVNULL)
    print(avg_write)
    print(avg_read)
    print(avg_delete)


def show_cats(N):
    """ Store, select, show and delete N cat images """
    N = int(N)
    os.system("voltdb init --force")
    subprocess.Popen(["voltdb", "start"])
    time.sleep(5)
    os.system("sqlcmd < CAT_SETUP_TABLE")
    vdb_insert(N)
    vdb_select(N, do_show=True)
    vdb_delete(N)
    subprocess.call(["voltadmin", "shutdown"])


def help_and_exit():
    print("To Benchmark: 'python3 test_vdb.py [singe, multi]'")
    print("To Show N cat images: 'python3 test_vdb.py cats N'")
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
