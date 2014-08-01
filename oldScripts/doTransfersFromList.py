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

          isComplete = cmsDataFlowCleanUp.isCompleteRun(debug, inputDataFolder, afterString)

          if(isComplete == True):
	     print "run ",inputDataFolder," complete"

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
