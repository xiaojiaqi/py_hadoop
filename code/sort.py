#!/usr/bin/python
import random
from pyhadoop_io import *
import subprocess

class sort:
    def randomfilename(self):
        r = ""
        for i in range(0, 20):
            r += str (random.randint(0,9))
        offsetfilename = "resource/" + r
        return offsetfilename

    def InputSplit(self,  filename):
        resources = []
        v = filename.split(",");
        logging.debug (v)
        taskid   = v[0]
        filename = v[1]

        file = open(filename,"rb")
        file.seek(0,2)
        #  get file size
        totalsize = file.tell()

        offset = 0;
        endset = 0;

        # split the file
        while (True):
            seekset = offset + 5*1024*1024
            if seekset > totalsize:
                seekset = totalsize
            file.seek(seekset, 0)
            while (True):
                t = file.read(1)
                if (t != '\n' and t != ""):
                    continue
                else:
                    break
            endset = file.tell()
            offsetfilename = self.randomfilename()
            logging.debug( "%s %d %d", offsetfilename, offset, endset)
            r = []
            r.append(offsetfilename)
            r.append(filename)
            r.append(offset)
            r.append(endset)

            resources.append(r)

            if (t == ""):
                break
            offset = endset

        file.close()
        logging.debug  (resources)
        for i in resources:
            offsetfilename = i[0]
            savetofile(offsetfilename, i)
            savetoServer(taskid, "InputSplit", offsetfilename, offsetfilename)
        taskcomplete(taskid, "InputSplit")

    def Map(self, taskid):
        while (True):
            s = loadfromServer(taskid, "InputSplit", "")
            if (s == "no"):
                logging.info("Map completed")
                taskcomplete(taskid, "Map")
                break
            v = s.split()
            logging.debug(v[1])
            t = loadfromfile(v[2])
            filename = t[1]
            rawfile = self.loadfile(filename, t[2], t[3])
            v = rawfile.split()
            maps = {}
            for i in v:
                if (maps.has_key(i)):
                    x = maps[i]
                    maps[i] = x + 1
                else:
                    maps[i] = 1
            map2 = []
            for i in range(0,11):
                map2.append({})
            for i in maps:
                l = len(i)
                map2[l][ long(i)] = maps[i]

            for i in range(0,11):
                offsetfilename = self.randomfilename()
                savetofile(offsetfilename, map2[i])
                savetoServer(taskid, "Map", str(i), offsetfilename)
                logging.debug("put data Map %s   file %s", str(i), offsetfilename)
    def Reduce(self, taskid):
        while (True):
            logging.info("call Reduce")
            s = loadfromServer(taskid, "Map", "")
            if (s == "no"):
                taskcomplete(taskid, "Reduce")
                break
            v = s.split()
            logging.info(v)
            summap ={}
            partitionid = v[1]
            for i in v[2:]:
                logging.info(i)
                t = loadfromfile(i)
                for j in t.keys():
                    if summap.has_key(j):
                       orgsum = summap[j]
                       orgsum += t[j]
                       summap[j] = orgsum
                    else:
                       summap[j] = t[j]

            offsetfilename = self.randomfilename()

            keys = summap.keys()
            keys.sort()
            file = open(offsetfilename, 'w')
            for i in keys:
                file.write( str(i) + " " + str(summap[i]) + "\n")
            file.close( )
            savetoServer(taskid, "Reduce", partitionid, offsetfilename)

    def OutputFormat(self, taskid):
        logging.info("call OutputFormat")
        for i in range(0,12):
            s = loadfromServer(taskid, "Reduce", str(i))
            if (s == "no"):
                continue
            v = s.split()
            logging.info(v)
            filename = v[2]
            cmd = "cat " + filename + " >> data"
            logging.info(cmd)
            subprocess.call(cmd, shell=True ) 
        taskcomplete(taskid, "OutputFormat")

    def loadfile(self, filename, offset, endset):
        file = open(filename,"rb")
        file.seek(offset,0)
        s = file.read(endset - offset)
        return s

    def echo(self, args):
        logging.debug  ( "world count " + args)
