#!/usr/bin/env python
import os, time, sys, getopt, fcntl
import shutil
import json
import glob
import multiprocessing
from multiprocessing.pool import ThreadPool
import logging
import thread
import datetime
import fileinput
import socket
import filecmp
import cmsDataFlowCleanUp

def doCompleteRun(paths_to_watch, completeMergingThreshold, debug):
   checkSumDict   = dict()
   # Maximum number with pool option (< 0 == always)
   nWithPollMax = -1
   # Maximum number of threads to be allowed with the pool option
   nThreadsMax  = 50
   # Number of loops
   nLoops = 0
   while 1:
      thePool = ThreadPool(nThreadsMax)
      
      nLoops = nLoops + 1
      inputDataFolders = glob.glob(paths_to_watch)
      for nf in range(0, len(inputDataFolders)):
          inputDataFolder = inputDataFolders[nf]
	  theRunNumber  = ""
	  outputEndName = socket.gethostname()

	  inputDataFolderString = inputDataFolder.split('/')	  
	  if inputDataFolderString[len(inputDataFolderString)-1] == '':
	    theRunNumber          = inputDataFolderString[len(inputDataFolderString)-2]
          else:
	    theRunNumber          = inputDataFolderString[len(inputDataFolderString)-1] 

	  # reading the list of files in the given folder
          after = dict ([(f, None) for f in os.listdir (inputDataFolder)])     
          afterString = [f for f in after]

	  cmsDataFlowCleanUp.isCompleteRun(debug, inputDataFolder, afterString, completeMergingThreshold, theRunNumber, outputEndName)

paths_to_watch = "/store/lustre/mergeMacro/run229858"
completeMergingThreshold = 1.0
debug = 10

doCompleteRun(paths_to_watch, completeMergingThreshold, debug)
