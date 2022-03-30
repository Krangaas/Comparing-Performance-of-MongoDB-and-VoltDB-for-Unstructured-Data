import os, subprocess, time
from statistics import mean
from voltDB_utils import *

N_FILES = [1, 1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000, 9000, 10000]

def single_op(iterations=10):
    """
    Initialize empty database, then store, select and delete N cat images with single-operations.
    Finally, close down the database.
    """
    avg_write = []
    avg_read = []
    avg_delete = []

    init_db()

    for N in N_FILES:
        print("N_FILES", N)
        write_times = []
        read_times = []
        delete_times = []
        for i in range(iterations):
            print("     iteration", i)
            write_times.append(vdb_insert(N))
            read_times.append(vdb_select(N))
            delete_times.append(vdb_delete(N))
        avg_write.append(mean(write_times))
        avg_read.append(mean(read_times))
        avg_delete.append(mean(delete_times))

    subprocess.call(["voltadmin", "shutdown"], stdout=subprocess.DEVNULL)
    print("avg_write: ", avg_write)
    print("avg_read : ", avg_read)
    print("avg_del  : ", avg_delete)


def multi_op(iterations=3):
    """
    Initialize empty database, then store and delete N cat images with multi-operations.
    Finally, close down the database.
    """
    avg_write = []
    #avg_read = []
    avg_delete = []

    init_db()

    for N in N_FILES:
        print("N_FILES", N)
        write_times = []
        read_times = []
        delete_times = []
        for i in range(iterations):
            print("     iteration", i)
            write_times.append(vdb_insert(N))
            #read_times.append(vdb_multiselect())
            delete_times.append(vdb_multidelete())
        avg_write.append(mean(write_times))
        #avg_read.append(mean(read_times))
        avg_delete.append(mean(delete_times))

    subprocess.call(["voltadmin", "shutdown"], stdout=subprocess.DEVNULL)
    print("avg_write: ", avg_write)
    #print("avg_read : ", avg_read)
    print("avg_del  : ", avg_delete)


def show_cats(N):
    """ Store, select, show and delete N cat images. """
    N = int(N)

    init_db(suppress_output=False)

    vdb_insert(N)
    vdb_select(N, do_show=True)
    vdb_delete(N)
    subprocess.call(["voltadmin", "shutdown"])


def init_db(suppress_output=True):
    """ Initialize an empty database, then set up tables and procedures. """
    if suppress_output:
        stdout_subp = subprocess.DEVNULL
        stdout_sys = " > /dev/null"
    else:
        stdout_subp = None
        stdout_sys = " "

    subprocess.call(["voltadmin", "shutdown"], stdout=stdout_subp) # Make sure voltDB is not active
    os.system("voltdb init --force" + stdout_sys) # Initialize as empty
    subprocess.Popen(["voltdb", "start"], stdout=stdout_subp)
    time.sleep(5) # give voltDB time to initalize the DB before setting up the tables
    os.system("sqlcmd < CAT_SETUP_TABLE.sql" + stdout_sys)


def help_and_exit():
    """ Display help text and exit. """
    print("To Benchmark: 'python3 test_voltDB.py [singe, multi]'")
    print("To Show N cat images: 'python3 test_voltDB.py cats N'")
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
