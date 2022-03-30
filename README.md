# INF-3701-Assignment-2

### Demonstration:
A simple demonstration can be found here:
```
https://youtu.be/1atx4SlLoTQ
```

The following scripts require both VoltDB and MongoDB to be installed. The media files should be extracted to a folder named *catfolder* in the same directory as the scripts.


### Using the VoltDB test script:
All interaction with VoltDB is done through the *test_voltDB.py* script. To display the help text, write the following in the terminal.
```
python3 test_voltDB.py
```
The VoltDB test script will automatically initialize an empty database and set up the required tables and procedures.
After a test has finished the database will automatically close.
The database serves on the default port: **21212**


### Using the MongoDB test script:
All interaction with VoltDB is done through the *test_mongoDB.py* script. To display the help text, write the following in the terminal.
```
python3 test_mongoDB.py
```
The MongoDB test script requires an instance of mongoDB to be running in order to run the tests.
The database must serve on the default port: **27017**

To start a mongoDB server, type the following in a terminal:
```
sudo systemctl start mongod
```
To shut down the server:
```
sudo systemctl stop mongod
```

### Installing MongoDB
To install MongoDB, perform steps 1-3 described in the following link:
```
https://linuxize.com/post/how-to-install-mongodb-on-ubuntu-20-04/
```

### Installing VoltDB
To install VoltDB perform the steps described in the following link:
```
https://github.com/VoltDB/voltdb/wiki/Building-VoltDB
```
**NOTE**: Please make sure that the script *voltdbclient.py* is in the same folder as *voltDB_utils.py* and *test_voltDB.py*. The VoltDB client script is included here, but is also available in the VoltDB install directory at:
```
voltdb/lib/python/voltdbclient.py
```


### Utilities
The scripts *mongoDB_utils.py* and *voltDB_utils.py* contain utility methods for tests to function correctly and should only be used through the test scripts.
