#!/usr/bin/env python
import os, time, sys, getopt
import shutil
import json
import glob
import logging
import fileinput
import socket
import filecmp

from Logging import getLogger
log = getLogger()

"""
Read json files
"""
def readJsonFile(inputJsonFile, debug):
   try:
      settingsLS = "bad"
      if(os.path.getsize(inputJsonFile) > 0):
         try:
            settings_textI = open(inputJsonFile, "r").read()
            if(float(debug) >= 50): log.info("trying to load: {0}".format(inputJsonFile))
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

def doTheChecking(paths_to_watch, path_eol, mergeType, debug):
   eventsIDict     = dict()
   eventsEoLSDict  = dict()
   eventsLDict     = dict()
   EoLSProblemDict = dict()
   eventsFUBUDict  = dict()
   if(float(debug) >= 10): log.info("I will watch: {0}".format(paths_to_watch))

   inputDataFoldersNoSorted = glob.glob(paths_to_watch)
   inputDataFolders = sorted(inputDataFoldersNoSorted, reverse=True)
   if(float(debug) >= 20): log.info("inputDataFolders: {0}".format(inputDataFolders))
   for nf in range(0, len(inputDataFolders)):
       inputDataFolder = inputDataFolders[nf]

       inputDataFolderString = inputDataFolder.split('/')
       # if statement to allow ".../" or ... for input folders 
       if inputDataFolderString[len(inputDataFolderString)-1] == '':
         theRunNumber = inputDataFolderString[len(inputDataFolderString)-2]
       else:
         theRunNumber = inputDataFolderString[len(inputDataFolderString)-1] 

       after = dict()
       try:
   	  after = dict ([(f, None) for f in os.listdir (inputDataFolder)])
       except Exception, e:
   	  log.error("os.listdir operation failed: {0} - {1}".format(inputDataFolder,e))

       afterStringNoSorted = [f for f in after]
       afterString = sorted(afterStringNoSorted, reverse=False)
       if(float(debug) >= 50): log.info("afterString: {0}".format(afterString))

       # loop over JSON files, which will give the list of files to be merged
       for i in range(0, len(afterString)):
     	  if not afterString[i].endswith(".jsn"): continue
     	  if "index" in afterString[i]: continue
     	  if afterString[i].endswith("recv"): continue
     	  if "EoLS" in afterString[i]: continue
     	  if "BoLS" in afterString[i]: continue
     	  if "EoR" in afterString[i]: continue

   	  fileNameString = afterString[i].split('_')

     	  if(float(debug) >= 50): log.info("FILE: {0}".format(afterString[i]))
     	  inputJsonFile = os.path.join(inputDataFolder, afterString[i])
     	  if(float(debug) >= 50): log.info("inputJsonFile: {0}".format(inputJsonFile))

   	  settings = readJsonFile(inputJsonFile,debug)

   	  # avoid corrupted files or streamEvD files
     	  if("bad" in settings): continue

   	  # this is the number of input and output events, and the name of the dat file, something critical
     	  # eventsOutput is actually the total number of events to merge in the macromerged stage
   	  eventsInput	    = int(settings['data'][0])
   	  eventsOutput      = int(settings['data'][1])
   	  errorCode	    = 0
     	  file  	    = ""
     	  fileErrorString   = None
     	  fileSize	    = 0
   	  nFilesBU	    = 0
   	  eventsTotalInput  = 0
   	  checkSum	    = 0
   	  NLostEvents	    = 0
     	  transferDest      = "dummy"
     	  if mergeType == "mini":
   	     eventsOutputError = int(settings['data'][2])
     	     errorCode         = int(settings['data'][3])
     	     file	       = str(settings['data'][4])
     	     fileSize	       = int(settings['data'][5])
     	     checkSum	       = int(settings['data'][7])		 

     	     # processed events == input + error events
     	     eventsInput = eventsInput + eventsOutputError

     	     if fileNameString[2] == "streamError":
     		file		= str(settings['data'][6])
     		fileErrorString = file.split(',')
     	  else:
     	     errorCode        = int(settings['data'][2])
     	     file	      = str(settings['data'][3])
     	     fileSize	      = int(settings['data'][4])
     	     checkSum	      = int(settings['data'][5])
   	     nFilesBU	      = int(settings['data'][6])
   	     eventsTotalInput = int(settings['data'][7])
     	     NLostEvents      = int(settings['data'][8])

   	  key = (fileNameString[0],fileNameString[1],fileNameString[2])
   	  if key in eventsIDict.keys():

     	     eventsInput = eventsIDict[key][0] + eventsInput
     	     eventsIDict[key].remove(eventsIDict[key][0])
     	     eventsIDict.update({key:[eventsInput]})

     	     NLostEvents = eventsLDict[key][0] + NLostEvents
     	     eventsLDict[key].remove(eventsLDict[key][0])
     	     eventsLDict.update({key:[NLostEvents]})

     	  else:

     	     eventsIDict.update({key:[eventsInput]})

     	     eventsLDict.update({key:[NLostEvents]})

     	  if mergeType == "mini": 
   	     keyEoLS = (fileNameString[0],fileNameString[1])
     	     if keyEoLS not in eventsEoLSDict.keys():
     		EoLSName = path_eol + "/" + fileNameString[0] + "/" + fileNameString[0] + "_" + fileNameString[1] + "_EoLS.jsn"
   		if(float(debug) >= 20): log.info("EoLSName: {0}".format(EoLSName))
   		if os.path.exists(EoLSName) and os.path.getsize(EoLSName) > 0:
   		   inputEoLSName = open(EoLSName, "r").read()
   		   settingsEoLS  = json.loads(inputEoLSName)
   		   eventsEoLS	 = int(settingsEoLS['data'][0])
   		   filesEoLS	 = int(settingsEoLS['data'][1])
   		   eventsAllEoLS = int(settingsEoLS['data'][2])
     		   NLostEvents   = int(settingsEoLS['data'][3])
     		   eventsEoLSDict.update({keyEoLS:[eventsEoLS,filesEoLS,eventsAllEoLS,NLostEvents]})
   		else:
     		   if (float(debug) >= 30): print "PROBLEM WITH EoLSFile: ",EoLSName
		   keyEoLSRun = (fileNameString[0])
		   if keyEoLSRun not in EoLSProblemDict.keys():
		      EoLSProblemDict.update({keyEoLSRun:["bad"]})

   	     if keyEoLS in eventsEoLSDict.keys():
     		if(float(debug) >= 10): log.info("mini-EventsEoLS/EventsInput-run/ls/stream: {0}, {1} - {2}".format(eventsEoLSDict[keyEoLS][0],eventsIDict[key][0],key))
   		if(eventsEoLSDict[keyEoLS][0] == eventsIDict[key][0]):
   		   if(float(debug) >= 10): log.info("Events match: {0}".format(key))
   		else:
   		   if (float(debug) >= 20):
   		       log.info("Events number does not match: EoL says {0} we have in the files: {1}".format(eventsEoLSDict[keyEoLS][0], eventsIDict[key][0]))
   	     else:
   		   if (float(debug) >= 20):
   		       log.warning("Looks like {0} is not in eventsEoLSDict".format(key))

   	  else:
     	     if(float(debug) >= 10): log.info("macro-EventsTotalInput/EventsInput/NLostEvents-run/ls/stream: {0}, {1}, {2} - {3}".format(eventsTotalInput,eventsIDict[key][0],eventsLDict[key][0],key))
   	     if(eventsTotalInput == (eventsIDict[key][0]+eventsLDict[key][0])):
   		if(float(debug) >= 10): log.info("Events match: {0}".format(key))
   	     else:
   		if (float(debug) >= 20):
   		    log.info("Events number does not match: EoL says {0}, we have in the files: {1} + {2} = {3}".format(eventsTotalInput, eventsIDict[key][0], eventsLDict[key][0], eventsIDict[key][0]+eventsLDict[key][0]))


   EoRFileName = path_eol + "/" + theRunNumber + "/" + theRunNumber + "_ls0000_EoR.jsn"
   if(os.path.exists(EoRFileName) and os.path.getsize(EoRFileName) > 0):
      settingsEoR = readJsonFile(EoRFileName, debug)

      if("bad" not in settingsEoR):

   	 eventsInputBU = int(settingsEoR['data'][0])

   	 eventsInputFU = 0
   	 for nb in range(0, len(afterString)):
   	    if not afterString[nb].endswith(".jsn"): continue
   	    if "index" in afterString[nb]: continue
   	    if afterString[nb].endswith("recv"): continue
   	    if "EoLS" in afterString[nb]: continue
   	    if "BoLS" in afterString[nb]: continue
   	    if not "EoR" in afterString[nb]: continue
   	    inputEoRFUJsonFile = os.path.join(inputDataFolder, afterString[nb])
   	    settingsLS = readJsonFile(inputEoRFUJsonFile, debug)

   	    if("bad" in settingsLS): continue

   	    eventsInputFU = eventsInputFU + int(settingsLS['data'][0])

         if theRunNumber not in eventsFUBUDict.keys():
            eventsFUBUDict.update({theRunNumber:[eventsInputFU,eventsInputBU]})

   for key in eventsIDict.keys():
      log.info("run/ls/stream incomplete: {0}".format(key))           

   for key in eventsFUBUDict.keys():
      log.info("{0}: events given by the FUs = {1} / events expected by the BUs = {2}".format(key,eventsFUBUDict[key][0],eventsFUBUDict[key][1]))           

   for keyEoLSRun in EoLSProblemDict.keys():
      log.info("run without EoLS folder found: {0}".format(keyEoLSRun))           

"""
Main
"""
valid = ['paths_to_watch=', 'path_eol=', 'mergeType=', 'debug=', 'help']

usage =  "Usage: listdir.py --paths_to_watch=<paths_to_watch>\n"
usage += "                  --path_eol=<eoldir>\n"
usage += "                  --mergeType=<mini/macro/auto>\n"
usage += "                  --debug=<0>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

paths_to_watch = "/fff/output/runxxxxx"
path_eol       = "/fff/ramdisk"
mergeType      = "mini"
debug          = 0

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--paths_to_watch":
      paths_to_watch = arg
   if opt == "--path_eol":
      path_eol = arg
   if opt == "--mergeType":
      mergeType = arg
   if opt == "--debug":
      debug = arg

if not os.path.exists(paths_to_watch):
   msg = "Run folder Not Found: %s" % paths_to_watch
   raise RuntimeError, msg

if not os.path.exists(path_eol) and mergeType == "mini":
   msg = "End of Lumi main folder Not Found: %s" % path_eol
   raise RuntimeError, msg

doTheChecking(paths_to_watch, path_eol, mergeType, debug)
