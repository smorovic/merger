#!/usr/bin/env python
import os, time, sys, getopt, fcntl
import shutil
import json
import glob
import multiprocessing
from multiprocessing.pool import ThreadPool
import thread
import datetime
import cmsDataFlowCleanUp

def doTheTransfers(paths_to_watch, debug):

   while 1:
      #print paths_to_watch
      inputDataFolders = glob.glob(paths_to_watch)
      if(float(debug) > 0): print "***************NEW LOOP***************"
      if(float(debug) > 0): print inputDataFolders
      for nf in range(0, len(inputDataFolders)):
          inputDataFolder = inputDataFolders[nf]
	  
	  # reading the list of files in the given folder
          before = dict ([(f, None) for f in os.listdir (inputDataFolder)])
          after = dict ([(f, None) for f in os.listdir (inputDataFolder)])     
          afterString = [f for f in after]
          added = [f for f in after if not f in before]
          removed = [f for f in before if not f in after]

          #isComplete = cmsDataFlowCleanUp.isCompleteRun(debug, inputDataFolder, afterString)

          #if(isComplete == True):
	  #   print "run ",inputDataFolder," complete"
###
   	  for nb in range(0, len(afterString)):
   	     if not afterString[nb].endswith(".jsn"): continue
   	     if "index" in afterString[nb]: continue
   	     if "streamError" in afterString[nb]: continue
   	     if afterString[nb].endswith("recv"): continue
   	     if "EoLS" in afterString[nb]: continue
   	     if "BoLS" in afterString[nb]: continue
      	     inputEoRJsonFile = os.path.join(inputDataFolder, afterString[nb])
      	     settingsLS_textI = open(inputEoRJsonFile, "r").read()
      	     settingsLS = json.loads(settingsLS_textI)
             events = int(settingsLS['data'][1])
             fileSize = int(settingsLS['data'][4])
	     if(os.path.getsize(inputEoRJsonFile.replace("jsn","dat")) < fileSize and events != 0):
	        print os.path.join(inputDataFolder, afterString[nb]),os.path.getsize(inputEoRJsonFile.replace("jsn","dat")),"jsn-info: ",fileSize,events
###
          before = after

"""
Main
"""
valid = ['paths_to_watch=', 'debug=', 'help']

usage =  "Usage: listdir.py --paths_to_watch=<paths_to_watch>\n"
usage += "                  --debug=<0>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

paths_to_watch = "unmerged"
debug          = 0

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--paths_to_watch":
      paths_to_watch = arg
   if opt == "--debug":
      debug = arg

doTheTransfers(paths_to_watch, debug)
