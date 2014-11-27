#!/usr/bin/python

import sys
from pyhadoop_io import *

def run_task(modename, functionname,arg):
    obj = __import__(modename) # import module
    c = getattr(obj,modename)
    obj = c() # new class
    fun = getattr(obj,functionname)
    fun(arg) # call def

if __name__ == '__main__':
    logging.debug(sys.argv)
    logging.debug(len(sys.argv))
    run_task(sys.argv[1], sys.argv[2], sys.argv[3])
    