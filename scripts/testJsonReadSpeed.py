#!/usr/bin/env python
import os, time, sys, getopt
import shutil
import json
import glob
import datetime
import logging
import cmsDataFlowMerger

from Logging import getLogger
log = getLogger()

def doReading(theInput,theMaxTime,theTooSlowTime,theDebug):

   initReadingTime = time.time()
   endReadingTime = 0
   totalReadFiles    = 0
   totalTimeFiles    = 0
   totalTooSlowFiles = 0
   while 1:
   
      endReadingTime = time.time()
      diffTime = endReadingTime-initReadingTime
      if(theMaxTime > 0 and diffTime > theMaxTime):
         msg  = "Maximum time (%f sec) has passed %f sec\n" % (diffTime,theMaxTime)
         msg += "Average time: %f msec\n" % (totalTimeFiles/totalReadFiles)
         msg += "Total too slow read files(%f msec): %d out of %d\n" % (theTooSlowTime,totalTooSlowFiles,totalReadFiles)
         print msg
         return

      inputDataFolders = glob.glob(theInput)

      if(debug >= 0): log.info("***************NEW LOOP***************")
      if(debug > 0): log.info("inputDataFolders: {0}".format(inputDataFolders))
      for nf in range(0, len(inputDataFolders)):
          inputDataFolder = inputDataFolders[nf]
	  
	  after = dict()
	  listFolders = sorted(glob.glob(os.path.join(inputDataFolder, 'stream*')));
	  for nStr in range(0, len(listFolders)):
             try:
        	after_temp = dict ([(f, None) for f in glob.glob(os.path.join(listFolders[nStr], '*.jsn'))])
        	after.update(after_temp)
        	after_temp = dict ([(f, None) for f in glob.glob(os.path.join(listFolders[nStr], '*.ini'))])
        	after.update(after_temp)
        	after_temp = dict ([(f, None) for f in glob.glob(os.path.join(listFolders[nStr], 'jsns', '*.jsn'))])
        	after.update(after_temp)
        	after_temp = dict ([(f, None) for f in glob.glob(os.path.join(listFolders[nStr], 'data', '*.ini'))])
        	after.update(after_temp)
             except Exception, e:
        	log.error("glob.glob operation failed: {0} - {1}".format(inputDataFolder,e))

	  afterStringNoSorted = [f for f in after]
	  afterString = sorted(afterStringNoSorted, reverse=False)

          for i in range(0, len(afterString)):
	     if not afterString[i].endswith(".jsn"): continue
	     if "index" in afterString[i]: continue
	     if afterString[i].endswith("recv"): continue
	     if "EoLS" in afterString[i]: continue
	     if "BoLS" in afterString[i]: continue
	     if "EoR" in afterString[i]: continue

	     inputJsonFile = os.path.join(inputDataFolder, afterString[i])

             initJsonTime = time.time()
             settings = str(cmsDataFlowMerger.readJsonFile(inputJsonFile, theDebug))
             endJsonTime = time.time()

             # avoid corrupted files
	     if("bad" in settings): continue
	     diffProcessTime = (endJsonTime-initJsonTime)*1000
	     totalReadFiles = totalReadFiles + 1
	     totalTimeFiles = totalTimeFiles + diffProcessTime
	     if(diffProcessTime > tooSlowTime): totalTooSlowFiles = totalTooSlowFiles + 1
             if(theDebug >= 1): log.info("Time in ms({0}): {1:5.1f}".format(inputJsonFile,diffProcessTime))

             endReadingTime = time.time()
             diffTime = endReadingTime-initReadingTime
             if(theMaxTime > 0 and diffTime > theMaxTime):
                break

   endReadingTime = time.time()
   diffTime = endReadingTime-initReadingTime
   if(theMaxTime > 0 and diffTime > theMaxTime):
      msg  = "Maximum time (%f sec) has passed %f sec\n" % (diffTime,theMaxTime)
      msg += "Average time: %f msec\n" % (totalTimeFiles/totalReadFiles)
      msg += "Total too slow read files(%f msec): %d out of %d\n" % (theTooSlowTime,totalTooSlowFiles,totalReadFiles)
      print msg
      return

"""
Main
"""
valid = ['input=', 'maxTime=', 'tooSlowTime=', 'debug=', 'help']

usage =  "Usage: testJsonReadSpeed.py --input=<input_folder>\n"
usage += "                            --maxTime<10 sec>\n"
usage += "                            --tooSlowTime<10 msec>\n"
usage += "                            --debug<0>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

input        = "dummy"
maxTime      = 10
tooSlowTime = 10.0
debug        = 0

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--input":
      input = arg
   if opt == "--maxTime":
      maxTime = float(arg)
   if opt == "--tooSlowTime":
      tooSlowTime = float(arg)
   if opt == "--debug":
      debug = int(arg)

if not os.path.exists(input):
   msg = "input folder not Found: %s" % input
   raise RuntimeError, msg

doReading(input,maxTime,tooSlowTime,debug)
