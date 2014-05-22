#!/usr/bin/env python
import os, time, sys, getopt, fcntl
import shutil
import json
import glob

from Logging import getLogger
log = getLogger()

"""
clean up run folder if some conditions are met
"""
def cleanUpRun(debug, EoRFileName, inputDataFolder, afterString, path_eol, theRunNumber):
   
   settingsEoR = ""
   print EoRFileName
   try:
      settingsEoR_textI = open(EoRFileName, "r").read()
      if(float(debug) >= 50): log.info("trying to load EoR file: {0}".format(EoRFileName))
      settingsEoR = json.loads(settingsEoR_textI)
   except ValueError, e:
      log.warning("Looks like the EoR file {0} is not available, I'll try again...".format(EoRFileName))
      try:
   	 time.sleep (0.1)
   	 settingsEoR_textI = open(EoRFileName, "r").read()
         settingsEoR = json.loads(settingsEoR_textI)
      except ValueError, e:
   	 log.warning("Looks like the EoR file {0} is not available (2nd try)...".format(EoRFileName))
   	 time.sleep (1.0)
   	 settingsEoR_textI = open(EoRFileName, "r").read()
         settingsEoR = json.loads(settingsEoR_textI)
   eventsTotalEoR = int(settingsEoR['data'][0])

   eventsInputFU = 0
   for nb in range(0, len(afterString)):
      if not afterString[nb].endswith(".jsn"): continue
      if "index" in afterString[nb]: continue
      if afterString[nb].endswith("recv"): continue
      if "EoLS" in afterString[nb]: continue
      if "BoLS" in afterString[nb]: continue
      if not "EoR" in afterString[nb]: continue
      if "TEMP" in afterString[nb]: continue
      inputEoRFUJsonFile = os.path.join(inputDataFolder, afterString[nb])
      settingsLS = ""
      try:
   	 settingsLS_textI = open(inputEoRFUJsonFile, "r").read()
   	 if(float(debug) >= 50): log.info("trying to load: {0}".format(inputEoRFUJsonFile))
   	 settingsLS = json.loads(settingsLS_textI)
      except ValueError, e:
   	 log.warning("Looks like the file {0} is not available, I'll try again...".format(inputEoRFUJsonFile))
   	 try:
   	    time.sleep (0.1)
   	    settingsLS_textI = open(inputEoRFUJsonFile, "r").read()
            settingsLS = json.loads(settingsLS_textI)
   	 except ValueError, e:
   	    log.warning("Looks like the file {0} is not available (2nd try)...".format(inputEoRFUJsonFile))
   	    time.sleep (1.0)
   	    settingsLS_textI = open(inputEoRFUJsonFile, "r").read()
            settingsLS = json.loads(settingsLS_textI)

      eventsInputFU = eventsInputFU + int(settingsLS['data'][0])
   
   if(float(debug) >= 50): log.info("eventsTotalEoR vs. eventsInputFU: {0} vs. {1}".format(eventsTotalEoR,eventsInputFU))
   if (eventsTotalEoR == eventsInputFU):
      numberBoLSFiles = 0
      for nb in range(0, len(afterString)):
   	 if not afterString[nb].endswith("_BoLS.jsn"): continue
   	 numberBoLSFiles = numberBoLSFiles + 1
      if(float(debug) >= 50): log.info("numberBoLSFiles: {0}".format(numberBoLSFiles))
      
      if(numberBoLSFiles == 0):
         EoLSFolder = os.path.join(path_eol, theRunNumber)
         log.info("Run folder deletion is triggered!: {0} and {1}".format(inputDataFolder,EoLSFolder))
         shutil.rmtree(inputDataFolder)
         time.sleep(10)
         shutil.rmtree(EoLSFolder)
