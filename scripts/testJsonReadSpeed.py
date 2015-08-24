#!/usr/bin/env python
import os, time, sys, getopt
import shutil
import json
import glob
import datetime
import logging

from Logging import getLogger
log = getLogger()

"""
Read json files
"""
def readJsonFile(inputJsonFile):
   try:
      settingsLS = "bad"
      if(os.path.getsize(inputJsonFile) > 0):
         try:
            settings_textI = open(inputJsonFile, "r").read()
            settingsLS = json.loads(settings_textI)
         except Exception, e:
            log.warning("Looks like the file {0} is not available, I'll try again...".format(inputJsonFile))
            try:
               time.sleep (0.1)
               settings_textI = open(inputJsonFile, "r").read()
   	       settingsLS = json.loads(settings_textI)
            except Exception, e:
               log.warning("Looks like the file {0} is not available (2nd try)...".format(inputJsonFile))
               try:
                  time.sleep (1.0)
                  settings_textI = open(inputJsonFile, "r").read()
   	          settingsLS = json.loads(settings_textI)
               except Exception, e:
                  log.warning("Looks like the file {0} failed for good (3rd try)...".format(inputJsonFile))
                  inputJsonFailedFile = inputJsonFile.replace("_TEMP.jsn","_FAILED.bad")
                  os.rename(inputJsonFile,inputJsonFailedFile)

      return settingsLS
   except Exception, e:
      log.error("readJsonFile {0} failed {1}".format(inputJsonFile.e))

def doReading(theInput,theMaxTime,theTooSlowTime,theDebug):

   initReadingTime = time.time()
   totalReadFiles    = 0
   totalTimeFiles    = 0
   totalTooSlowFiles = 0
   while 1:
   
      endReadingTime = time.time()
      diffTime = endReadingTime-initReadingTime
      if(theMaxTime > 0 and diffTime > theMaxTime):
         msg  = "Maximum time (%f) has passed %f\n" % (diffTime,theMaxTime)
         msg += "Average time: %f msec\n" % (totalTimeFiles/totalReadFiles)
         msg += "Total too slow read files(%f msec): %d out of %d\n" % (theTooSlowTime,totalTooSlowFiles,totalReadFiles)
         raise RuntimeError, msg

      inputDataFolders = glob.glob(theInput)

      if(debug >= 0): log.info("***************NEW LOOP***************")
      if(debug > 0): log.info("inputDataFolders: {0}".format(inputDataFolders))
      for nf in range(0, len(inputDataFolders)):
          inputDataFolder = inputDataFolders[nf]
	  
          after = dict()
          try:
             after = dict ([(f, None) for f in os.listdir (inputDataFolder)])
          except Exception, e:
             log.error("os.listdir operation failed: {0} - {1}".format(inputDataFolder,e))
          afterString = [f for f in after]

          for i in range(0, len(afterString)):
	     if not afterString[i].endswith(".jsn"): continue
	     if "index" in afterString[i]: continue
	     if afterString[i].endswith("recv"): continue
	     if "EoLS" in afterString[i]: continue
	     if "BoLS" in afterString[i]: continue
	     if "EoR" in afterString[i]: continue

	     inputJsonFile = os.path.join(inputDataFolder, afterString[i])

             initJsonTime = time.time()
             settings = readJsonFile(inputJsonFile)
             endJsonTime = time.time()

             # avoid corrupted files
	     if("bad" in settings): continue
	     diffProcessTime = (endJsonTime-initJsonTime)*1000
	     totalReadFiles = totalReadFiles + 1
	     totalTimeFiles = totalTimeFiles + diffProcessTime
	     if(diffProcessTime > tooSlowTime): totalTooSlowFiles = totalTooSlowFiles + 1
             if(theDebug >= 1): log.info("Time in ms({0}): {1:5.1f}".format(inputJsonFile,diffProcessTime))

"""
Main
"""
valid = ['input=', 'maxTime=', 'tooSlowTime=', 'debug=', 'help']

usage =  "Usage: testJsonReadSpeed.py --input=<input_folder>\n"
usage += "                            --maxTime<-1 sec>\n"
usage += "                            --tooSlowTime<10 msec>\n"
usage += "                            --debug<0>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

input        = "dummy"
maxTime      = -1.0
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
