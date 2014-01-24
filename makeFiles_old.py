#!/usr/bin/env python

import os, time, sys, getopt
import random
import json

"""
Do actual files
"""
def doFiles(RUNNumber, seeds, timeEnd, rate, path_to_make, streamName, sizePerFile):
   random.seed(int(seeds))
   theNLoop = 1
   nInput = 0
   nOutput = 0
   LSNumber = 0

   start = time.time()
   while (float(timeEnd) < 0.0 or float(time.time() - start) < float(timeEnd)):
     time.sleep (float(rate))

     # just in case we need more than one seed
     numberOfSeedsNeeded = 1
     seedsRND = []
     for i in range(0, numberOfSeedsNeeded):
       seedsRND.append(random.randint(0,999999))

     nInput += 100
     nOutput += 9
     fileName =  "%s/Data.%d.LS%d.%s.%d.std.dat" % (path_to_make,RUNNumber,LSNumber,streamName,seedsRND[0])
     thefile = open(fileName, 'w')
     thefile.write('0' * 1024 * 1024 * int(sizePerFile))
     thefile.write("\n")
     thefile.close()

     fileName =  "%s/Data.%d.LS%d.%s.%d.mon.dat" % (path_to_make,RUNNumber,LSNumber,streamName,seedsRND[0])
     thefile = open(fileName, 'w')
     thefile.write('0' * 1024 * 1024 * 1)
     thefile.write("\n")
     thefile.close()

     fileName =  "%s/Data.%d.LS%d.%s.%d.met.json" % (path_to_make,RUNNumber,LSNumber,streamName,seedsRND[0])
     thefile = open(fileName, 'w')
     thefile.write(json.dumps({'inputEvents': 100, 'outputEvents': 9}, sort_keys=True, indent=4, separators=(',', ': ')))
     thefile.close()

     if(theNLoop%30 == 0):
	fileName =  "%s/Data.%d.LS%d.%s.0000.eol.json" % (path_to_make,RUNNumber,LSNumber,streamName)
	thefile = open(fileName, 'w')
	thefile.write(json.dumps({'inputEvents': int(nInput), 'outputEvents': int(nOutput)}, sort_keys=True, indent=4, separators=(',', ': ')))
	thefile.close()
	nInput = 0
	nOutput = 0
	LSNumber += 1

     if(theNLoop%1000 == 0):
	nInput = 0
	nOutput = 0
	LSNumber += 1
	RUNNumber += 1

     theNLoop += 1

"""
Main
"""

def createFiles(streamName = "StreamA", sizePerFile = 50, path_to_make = "unmerged", rate = 0.0, seeds = 999, timeEnd = -1, RUNNumber = 100):
    
    if not os.path.exists(path_to_make):
       os.mkdir(path_to_make)
    
    doFiles(int(RUNNumber), seeds, timeEnd, rate, path_to_make, streamName, sizePerFile)
