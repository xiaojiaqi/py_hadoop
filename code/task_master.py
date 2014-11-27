#!/usr/bin/python

import socket
import sys
#import logging
from pyhadoop_io import *
import subprocess

from datetime import datetime
dstart=""
dend  =""

host = ''
port = 8990
maptasksize = 30
reducetasksize = 30
outputtasksize = 1

TaskData = {}

if __name__ == '__main__':
    taskid    = ""
    inputfile = ""
    modulename = ""
    logging.debug(sys.argv)
    if len(sys.argv) >= 5:
        taskid = sys.argv[1]
        inputfile = sys.argv[2]
        modulename = sys.argv[3]
        processname = sys.argv[4]
    else:
        sys.exit(-1)
    dstart = datetime.now()
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((host, port))
    server.listen(100)

    subprocess.Popen("./pyhadoop_task.py " +  modulename + " " +  processname + " " +  taskid  + "," + inputfile, shell=True, stdin=None, stdout=None, stderr=None)
    #sample "mytask 1.txt sort InputSplit"


    TaskData[taskid] = {}
    s = TaskData[taskid]
    s[processname] = 1
    s["-" + processname] = 0

    while (True):
        client, clientaddr = server.accept()
        #logging.debug  ( "Got connections from ", client.getpeername())
        data = client.recv(1024)
        logging.debug  ( data )
        v = data.split()
        logging.debug  ( v )

        taskid = v[1]
        processname = v[2]
        logging.debug("task = %s  processname = %s", taskid, processname)

        if not TaskData.has_key(taskid) :
            client.close()
            continue
        record = TaskData[taskid]
        if not record.has_key(processname):
            client.close()
            continue
        v1 = record[processname]
        key = "-" + processname
        v2 = record[key]
        v2 += 1
        record[key] = v2
        if (v1 == v2):
            logging.debug("all task finished %s %s %d", taskid, processname, v1)
            if (processname == "InputSplit"):
                # InputSplit is ok run map
                processname = "Map"
                s = TaskData[taskid]
                s[processname] = maptasksize
                s["-" + processname] = 0

                for i in range(0,maptasksize):
                    subprocess.Popen("./pyhadoop_task.py " +  modulename + " " +  processname + " " + taskid, shell=True, stdin=None, stdout=None, stderr=None)

            elif (processname == "Map"):
                processname = "Reduce"
                s = TaskData[taskid]
                s[processname] = reducetasksize
                s["-" + processname] = 0
                for i in range(0, reducetasksize):
                    cmd = "./pyhadoop_task.py " +  modulename + " " +  processname + " " + taskid
                    logging.info("%s", cmd)
                    subprocess.Popen(cmd , shell=True, stdin=None, stdout=None, stderr=None)
            elif (processname == "Reduce"):
                processname = "OutputFormat"
                s = TaskData[taskid]
                s[processname] = outputtasksize
                s["-" + processname] = 0
                for i in range(0, outputtasksize):
                    cmd = "./pyhadoop_task.py " +  modulename + " " +  processname + " " + taskid
                    logging.info("%s", cmd)
                    subprocess.Popen(cmd , shell=True, stdin=None, stdout=None, stderr=None)
            elif (processname == "OutputFormat"):
                    logging.info("GAME OVER");
                    dend = datetime.now()
                    print dend - dstart
                    sys.exit(0)










