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
def cleanUpRun(debug, EoRFileName, inputDataFolder, afterString, path_eol, theRunNumber, outputSMMergedFolder, outputEndName, completeMergingThreshold):
   
   settingsEoR = ""
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
   eventsInputBU = int(settingsEoR['data'][0])

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
   
   if(float(debug) >= 50): log.info("eventsInputBU vs. eventsInputFU: {0} vs. {1}".format(eventsInputBU,eventsInputFU))
   if (eventsInputBU*completeMergingThreshold <= eventsInputFU):
      numberBoLSFiles = 0
      for nb in range(0, len(afterString)):
   	 if not afterString[nb].endswith("_BoLS.jsn"): continue
         if "DQM" in afterString[nb]: continue
         if "streamError" in afterString[nb]: continue
   	 numberBoLSFiles = numberBoLSFiles + 1
      if(float(debug) >= 50): log.info("numberBoLSFiles: {0}".format(numberBoLSFiles))
      
      EoLSFolder    = os.path.join(path_eol, theRunNumber)
      eventsEoLS          = [0, 0, 0]
      eventsEoLS_noLastLS = [0, 0, 0]
      lastLumiBU = doSumEoLS(EoLSFolder, eventsEoLS, eventsEoLS_noLastLS)

      if(eventsEoLS[0] != eventsInputBU):
         log.info("PROBLEM eventsEoLS != eventsInputBU: {0} vs. {1}".format(eventsEoLS[0],eventsInputBU))

      # This is needed to cleanUp the macroMerger later
      EoRFileNameMiniOutput	  = outputSMMergedFolder + "/../" + theRunNumber + "_ls0000_MiniEoR_" + outputEndName + ".jsn_TEMP"
      EoRFileNameMiniOutputStable = outputSMMergedFolder + "/../" + theRunNumber + "_ls0000_MiniEoR_" + outputEndName + ".jsn"

      theEoRFileMiniOutput = open(EoRFileNameMiniOutput, 'w')
      theEoRFileMiniOutput.write(json.dumps({'eventsInputBU':   eventsInputBU, 
                                             'eventsInputFU':   eventsInputFU, 
					     'numberBoLSFiles': numberBoLSFiles,
					     'eventsTotalRun':  eventsEoLS[1],
					     'eventsLostBU':    eventsEoLS[2],
                                             'eventsInputBU_noLastLS':   eventsEoLS_noLastLS[0], 
					     'eventsTotalRun_noLastLS':  eventsEoLS_noLastLS[1],
					     'eventsLostBU_noLastLS':    eventsEoLS_noLastLS[2],
					     'lastLumiBU':      lastLumiBU}))
      theEoRFileMiniOutput.close()

      shutil.move(EoRFileNameMiniOutput, EoRFileNameMiniOutputStable)

      if(numberBoLSFiles == 0 and eventsInputBU == eventsInputFU):
         EoLSFolder = os.path.join(path_eol, theRunNumber)
         log.info("Run folder deletion is triggered!: {0} and {1}".format(inputDataFolder,EoLSFolder))
         time.sleep(10)
         try:
            shutil.rmtree(inputDataFolder)
   	 except Exception,e:
   	    log.error("Failed removing {0} - {1}".format(inputDataFolder,e))
         try:
            if os.path.islink(EoLSFolder):
               link_dir = os.readlink(EoLSFolder)
               log.info("EoLS run directory is a symlink pointing to {0}".format(link_dir))
               os.unlink(EoLSFolder)
               EoLSFolder = link_dir
            shutil.rmtree(EoLSFolder)
   	 except Exception,e:
   	    log.error("Failed removing {0} - {1}".format(EoLSFolder,e))

def doSumEoLS(inputDataFolder, eventsEoLS, eventsEoLS_noLastLS):

   after = dict ([(f, None) for f in os.listdir (inputDataFolder)])     
   afterStringNoSorted = [f for f in after]

   afterString = sorted(afterStringNoSorted, reverse=True)
   numberLS = 0

   lastLumiBU = -1

   # total number of processed events in a given BU (not counting last LS)
   eventsEoLS_noLastLS[0] = 0
   # total number of processed events in all BUs (not counting last LS)
   eventsEoLS_noLastLS[1] = 0
   # total number of lost events in a given BU (not counting last LS)
   eventsEoLS_noLastLS[2] = 0

   # total number of processed events in a given BU
   eventsEoLS[0] = 0
   # total number of processed events in all BUs
   eventsEoLS[1] = 0
   # total number of lost events in a given BU
   eventsEoLS[2] = 0
   for nb in range(0, len(afterString)):
      if not afterString[nb].endswith("EoLS.jsn"): continue

      EoLSFileName = os.path.join(inputDataFolder, afterString[nb])

      if(os.path.exists(EoLSFileName) and os.path.getsize(EoLSFileName)):
         inputEoLSName = open(EoLSFileName, "r").read()
         settingsEoLS  = json.loads(inputEoLSName)
         
         if(int(settingsEoLS['data'][0]) > 0 or 
            int(settingsEoLS['data'][2]) > 0 or
            int(settingsEoLS['data'][3]) > 0):
            numberLS = numberLS + 1
            if numberLS != 1:
               eventsEoLS_noLastLS[0] += int(settingsEoLS['data'][0])
               eventsEoLS_noLastLS[1] += int(settingsEoLS['data'][2])
               eventsEoLS_noLastLS[2] += int(settingsEoLS['data'][3])

            else:
               fileNameString = EoLSFileName.split('_')
               try:
                  lastLumiBU = int(fileNameString[1].replace("ls",""))
               except Exception,e:
                  log.error("lastLumiBU assingment failed {0} - {1}".format(fileNameString[1],e))

            eventsEoLS[0] += int(settingsEoLS['data'][0])
            eventsEoLS[1] += int(settingsEoLS['data'][2])
            eventsEoLS[2] += int(settingsEoLS['data'][3])

   return lastLumiBU
