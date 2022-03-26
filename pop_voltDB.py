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

from matplotlib.pyplot import close
from numpy import size
from voltdbclient import *
import math

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

    N_FILES = 10
    PROCESSED_FILES = 0
    path = './catfolder/'
    cell_limit = 1000000
    row_limit = 2097000
    for filename in os.listdir(path):
        if filename.endswith("jpg") and PROCESSED_FILES < N_FILES:
            file = path + filename
            with open(file, 'rb') as f:
                contents = f.read()
                str_contents = str(contents)
                file_size = sys.getsizeof(str_contents)
                if file_size < row_limit:
                    s0 = str_contents[0:cell_limit]
                    if file_size > cell_limit:
                        s1 = str_contents[cell_limit:2*cell_limit]
                    else:
                        s1 = ""
                    if file_size > 2*cell_limit:
                        s2 = str_contents[2*cell_limit:file_size]
                    else:
                        s2 = ""
                    proc.call([filename, s0, s1, s2])
                    PROCESSED_FILES += 1

def select():
    client = FastSerializer("localhost", 21212)
    proc = VoltProcedure( client, "Select", [FastSerializer.VOLTTYPE_STRING])
    response = proc.call(["00000009_022.jpg"])
    #for x in response.tables:
    #    print(x)


if __name__=='__main__':
    insert()
