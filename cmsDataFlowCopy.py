#!/usr/bin/env python
import os, time, sys, getopt, fcntl
import shutil
import json
import glob
import multiprocessing
from multiprocessing.pool import ThreadPool
import thread
import datetime
import fileinput
import socket
import filecmp

from Logging import getLogger
log = getLogger()

# program to copy files given a list

"""
Do actual Copying
"""
def copyFiles(inputDataFolder, inputJsonFile, outMergedFile, outputMergedFolder,  doRemoveFiles, debug):

   inFileFullPath  = os.path.join(inputDataFolder,    outMergedFile)
   outFileFullPath = os.path.join(outputMergedFolder, outMergedFile)

   inJsonRenameFile = inputJsonFile.replace(".jsn","_TEMP.jsn")
   inJsonFileFullPath  = os.path.join(inputDataFolder, inJsonRenameFile)
   outJsonFileFullPath = os.path.join(outputMergedFolder, inputJsonFile)

   initCopyingTime = time.time()
   now = datetime.datetime.now()
   if(float(debug) > 0): log.info("{0}: Start copy of {1}".format(now.strftime("%H:%M:%S"), outJsonFileFullPath))

   if(doRemoveFiles == "True"):    
      shutil.move(inFileFullPath,outFileFullPath)
      shutil.move(inJsonFileFullPath,outJsonFileFullPath)
   else:
      shutil.copy(inFileFullPath,outFileFullPath)
      shutil.copy(inJsonFileFullPath,outJsonFileFullPath)

   outFileFullPathStable = outputMergedFolder + "/../" + outMergedFile
   outJSONFullPathStable = outputMergedFolder + "/../" + inputJsonFile

   # final location once we are done
   shutil.move(outFileFullPath,outFileFullPathStable)
   shutil.move(outJsonFileFullPath,outJSONFullPathStable)

   endCopyingTime = time.time() 
   now = datetime.datetime.now()
   if(float(debug) > 0): log.info("{0}, : Time for Copying({1}): {2}".format(now.strftime("%H:%M:%S"), outJsonFileFullPath, endCopyingTime-initCopyingTime))

"""
Do recovering JSON files
"""
def doTheRecovering(paths_to_watch, debug):
   inputDataFolders = glob.glob(paths_to_watch)
   log.info("Recovering: inputDataFolders: {0}".format(inputDataFolders))
   if(float(debug) >= 10): log.info("**************recovering JSON files***************")
   for nf in range(0, len(inputDataFolders)):
      inputDataFolder = inputDataFolders[nf]	   
      # reading the list of files in the given folder
      before = dict ([(f, None) for f in os.listdir (inputDataFolder)])
      after = dict ([(f, None) for f in os.listdir (inputDataFolder)])     
      afterString = [f for f in after]
      added = [f for f in after if not f in before]
      removed = [f for f in before if not f in after]

      # loop over JSON files, which will give the list of files to be recovered
      for i in range(0, len(afterString)):
         if "_TEMP.jsn" in afterString[i]:
            inputJsonFile = os.path.join(inputDataFolder, afterString[i])
            inputJsonRenameFile = inputJsonFile.replace("_TEMP.jsn",".jsn")
            shutil.move(inputJsonFile,inputJsonRenameFile)

"""
Do loops
"""
def doTheCopying(paths_to_watch, path_eol, debug, outputMerge, doRemoveFiles):
   if(float(debug) >= 10): log.info("I will watch: {0}".format(paths_to_watch))
   # Maximum number with pool option (< 0 == always)
   nWithPollMax = 0
   # Maximum number of threads to be allowed with the pool option
   nThreadsMax  = 50
   # Number of loops
   nLoops = 0
   while 1:
      thePool = ThreadPool(nThreadsMax)
      nLoops = nLoops + 1
      inputDataFolders = glob.glob(paths_to_watch)
      if(float(debug) >= 20): log.info("***************NEW LOOP************** {0}".format(nLoops))
      if(float(debug) >= 20): log.info("inputDataFolders: {0}".format(inputDataFolders))
      for nf in range(0, len(inputDataFolders)):
          inputDataFolder = inputDataFolders[nf]
	  # making output folders
	  inputDataFolderString = inputDataFolder.split('/')
	  # if statement to allow ".../" or ... for input folders 
	  if inputDataFolderString[len(inputDataFolderString)-1] == '':
	    outputMergedFolder    = os.path.join(outputMerge,    inputDataFolderString[len(inputDataFolderString)-2], "open")
	    theRunNumber          = inputDataFolderString[len(inputDataFolderString)-2]
          else:
	    outputMergedFolder    = os.path.join(outputMerge,    inputDataFolderString[len(inputDataFolderString)-1], "open")
	    theRunNumber         = inputDataFolderString[len(inputDataFolderString)-1] 

	  if not os.path.exists(outputMergedFolder):
             try:
                os.makedirs(outputMergedFolder)
             except OSError, e:
                 log.warning("Looks like the directory {0} has just been created by someone else...".format(outputMergedFolder))

	  # reading the list of files in the given folder
          before = dict ([(f, None) for f in os.listdir (inputDataFolder)])
          if(float(debug) >= 50): time.sleep (1)
          if(float(debug) >= 20): log.info("Begin folder iteration")
          after = dict ([(f, None) for f in os.listdir (inputDataFolder)])     
          afterString = [f for f in after]
          added = [f for f in after if not f in before]
          if(float(debug) >= 50): log.info("afterString: {0}".format(afterString))
          removed = [f for f in before if not f in after]
          if added: 
             if(float(debug) >= 50): log.info("Added: {0}".format(added))
          if removed: 
             if(float(debug) >= 50): log.info("Removed: {0}".format(removed))

	  # loop over JSON files, which will give the list of files to be copied
	  for i in range(0, len(afterString)):
	     if not afterString[i].endswith(".jsn"): continue
	     if "index" in afterString[i]: continue
	     if afterString[i].endswith("recv"): continue
	     if "EoLS" in afterString[i]: continue
	     if "BoLS" in afterString[i]: continue
	     if "EoR" in afterString[i]: continue
	     if "TEMP" in afterString[i]: continue
	     if(float(debug) >= 50): log.info("FILE: {0}".format(afterString[i]))
	     inputJsonFile = os.path.join(inputDataFolder, afterString[i])
	     if(float(debug) >= 50): log.info("inputJsonFile: {0}".format(inputJsonFile))

             # avoid empty files
	     if(os.path.getsize(inputJsonFile) == 0): continue

             # moving the file to avoid issues
	     inputJsonRenameFile = inputJsonFile.replace(".jsn","_TEMP.jsn")
             shutil.move(inputJsonFile,inputJsonRenameFile)

	     try:
                settings_textI = open(inputJsonRenameFile, "r").read()
                if(float(debug) >= 50): log.info("trying to load: {0}".format(inputJsonRenameFile))
	        settings = json.loads(settings_textI)
             except ValueError, e:
                log.warning("Looks like the file {0} is not available, I'll try again...".format(inputJsonRenameFile))
	        try:
	           time.sleep (0.1)
                   settings_textI = open(inputJsonRenameFile, "r").read()
		   settings = json.loads(settings_textI)
                except ValueError, e:
                   log.warning("Looks like the file {0} is not available (2nd try)...".format(inputJsonRenameFile))
	           time.sleep (1.0)
                   settings_textI = open(inputJsonRenameFile, "r").read()
		   settings = json.loads(settings_textI)

             fileNameString = afterString[i].split('_')

             # this is the number of input and output events, and the name of the dat file, something critical
	     # eventsOutput is actually the total number of events to merge in the macromerged stage
             eventsInput       = int(settings['data'][0])
             eventsOutput      = int(settings['data'][1])
             eventsOutputError = int(settings['data'][2])
	     errorCode         = int(settings['data'][3])
	     file	       = str(settings['data'][4])
	     fileSize	       = int(settings['data'][5])
	     if(float(debug) >= 50): log.info("Info from json file(eventsInput, eventsOutput, eventsOutputError, errorCode, file, fileSize): {0}, {1}, {2}, {3}, {4}, {5}".format(eventsInput, eventsOutput, eventsOutputError, errorCode, file, fileSize))
	     # processed events == input + error events
	     eventsInput = eventsInput + eventsOutputError
	
             if nLoops <= nWithPollMax or nWithPollMax < 0:
             	process = thePool.apply_async(             copyFiles,        [inputDataFolder, afterString[i], file, outputMergedFolder,  doRemoveFiles, debug])
	     else:
             	process = multiprocessing.Process(target = copyFiles, args = [inputDataFolder, afterString[i], file, outputMergedFolder,  doRemoveFiles, debug])
             	process.start()
          before = after

      if nLoops <= nWithPollMax or nWithPollMax < 0:
         thePool.close()
         thePool.join()

def start_copying(paths_to_watch, path_eol, outputMerge, doRemoveFiles, debug):

    if not os.path.exists(path_eol):
       msg = "End of Lumi folder Not Found: %s" % path_eol
       raise RuntimeError, msg
    
    if not os.path.exists(outputMerge):
       try:
          os.makedirs(outputMerge)
       except OSError, e:
          log.warning("Looks like the directory {0} has just been created by someone else...".format(outputMerge))

    doTheRecovering(paths_to_watch, debug)
    doTheCopying(paths_to_watch, path_eol, debug, outputMerge, doRemoveFiles)
