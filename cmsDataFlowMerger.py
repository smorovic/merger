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
import cmsActualMergingFiles

from Logging import getLogger
log = getLogger()

# program to merge (cat) files given a list

"""
Do actual merging
"""
def mergeFiles(outputMergedFolder, outputSMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, filesJSON, errorCode, typeMerging, doRemoveFiles, outputEndName, optionMerging, debug):

   if optionMerging == "optionA":
      cmsActualMergingFiles.mergeFilesA(outputMergedFolder,                       outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, filesJSON, errorCode, typeMerging, doRemoveFiles, outputEndName, debug)

   elif optionMerging == "optionB":
      cmsActualMergingFiles.mergeFilesB(outputMergedFolder, outputSMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, filesJSON, errorCode, typeMerging, doRemoveFiles, outputEndName, debug)

   elif optionMerging == "optionC":
      cmsActualMergingFiles.mergeFilesC(outputMergedFolder, outputSMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, filesJSON, errorCode, typeMerging, doRemoveFiles, outputEndName, debug)

   else:
      log.error("Wrong option!: {0}".format(optionMerging))
      msg = "Wrong option!: %s" % (optionMerging)
      raise RuntimeError, msg

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
         if "_TEMP.ini" in afterString[i]:
            inputIniFile = os.path.join(inputDataFolder, afterString[i])
            inputIniRenameFile = inputIniFile.replace("_TEMP.ini",".ini")
            shutil.move(inputIniFile,inputIniRenameFile)

"""
Check if file is completed
"""
def is_completed(filepath):
   """Checks if a file is completed by opening it in append mode.
   If no exception thrown, then the file is not locked.
   """
   completed = False
   file_object = None
   buffer_size = 8
   # Opening file in append mode and read the first 8 characters
   file_object = open(filepath, 'a', buffer_size)
   if file_object:
      completed = True
      file_object.close()

   return completed

"""
Do loops
"""
def doTheMerging(paths_to_watch, path_eol, typeMerging, debug, outputMerge, outputSMMerge, outputEndName, doRemoveFiles, optionMerging):
   filesDict      = dict() 
   errorCodeDict  = dict()    
   jsonsDict      = dict()    
   eventsIDict    = dict()
   eventsODict    = dict()
   eventsEoLSDict = dict()
   nFilesBUDict   = dict() 
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
	    outputMergedFolder   = os.path.join(outputMerge,   inputDataFolderString[len(inputDataFolderString)-2], "open")
	    outputSMMergedFolder = os.path.join(outputSMMerge, inputDataFolderString[len(inputDataFolderString)-2], "open")
          else:
	    outputMergedFolder   = os.path.join(outputMerge,   inputDataFolderString[len(inputDataFolderString)-1], "open")
	    outputSMMergedFolder = os.path.join(outputSMMerge, inputDataFolderString[len(inputDataFolderString)-1], "open")

	  if not os.path.exists(outputMergedFolder):
             try:
                os.makedirs(outputMergedFolder)
             except OSError, e:
                 log.warning("Looks like the directory {0} has just been created by someone else...".format(outputMergedFolder))
	  
	  if not os.path.exists(outputSMMergedFolder):
             try:
                os.makedirs(outputSMMergedFolder)
             except OSError, e:
                 log.warning("Looks like the directory {0} has just been created by someone else...".format(outputSMMergedFolder))

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

	  # loop over ini files, needs to be done first of all
          if typeMerging == "mini" or optionMerging == "optionA":
             theIniOutputFolder = outputSMMergedFolder
	     if optionMerging == "optionA":
                theIniOutputFolder = outputMergedFolder

	     for i in range(0, len(afterString)):

		if(afterString[i].endswith(".ini") and "TEMP" not in afterString[i]):
                   inputName  = os.path.join(inputDataFolder,afterString[i])
                   if (float(debug) >= 10): log.info("inputName: {0}".format(inputName))
                   if is_completed(inputName) == True and os.path.getsize(inputName) > 0:
		      # init name: runxxx_ls0000_streamY_HOST.ini
		      inputNameString = afterString[i].split('_')
                      # outputIniName will be modified in the next merging step immediately, while outputIniNameToCompare will stay forever
		      outputIniName          = theIniOutputFolder + "/../" + inputNameString[0] + "_ls0000_" + inputNameString[2] + "_" + outputEndName + ".ini"
                      outputIniNameToCompare = theIniOutputFolder +   "/"  + inputNameString[0] + "_ls0000_" + inputNameString[2] + "_" + outputEndName + ".ini"
		      inputNameRename  = inputName.replace(".ini","_TEMP.ini")
                      shutil.move(inputName,inputNameRename)
                      if(float(debug) >= 10): log.info("iniFile: {0}".format(afterString[i]))
	              # getting the ini file, just once per stream
		      if not os.path.exists(outputIniName) or os.path.getsize(outputIniName) == 0:
			 try:
                            with open(outputIniName, 'a', 1) as file_object:
                               fcntl.flock(file_object, fcntl.LOCK_EX)
		               shutil.copy(inputNameRename,outputIniName)
                               fcntl.flock(file_object, fcntl.LOCK_UN)
			    file_object.close()
			 except OSError, e:
		            log.warning("Looks like the outputIniName file {0} has just been created by someone else...".format(outputIniName))

		      if not os.path.exists(outputIniNameToCompare) or os.path.getsize(outputIniNameToCompare) == 0:
			 try:
                            with open(outputIniNameToCompare, 'a', 1) as file_object:
                               fcntl.flock(file_object, fcntl.LOCK_EX)
		               shutil.copy(inputNameRename,outputIniNameToCompare)
                               fcntl.flock(file_object, fcntl.LOCK_UN)
			    file_object.close()
			 except OSError, e:
		            log.warning("Looks like the outputIniNameToCompare file {0} has just been created by someone else...".format(outputIniNameToCompare))

	              # otherwise, checking if they are identical
	              else:
                	 try:
		            if filecmp.cmp(outputIniNameToCompare,inputNameRename) == False:
			       log.warning("ini files: {0} and {1} are different!!!".format(outputIniNameToCompare,inputNameRename))
                	 except IOError, e:
                               log.error("Try to move a .ini to a _TEMP.ini, disappeared under my feet. Carrying on...")

                      if(doRemoveFiles == "True"): 
                	 os.remove(inputNameRename)

		   else:
		      log.info("Looks like the file {0} is being copied by someone else...".format(inputName))

	  # loop over JSON files, which will give the list of files to be merged
	  for i in range(0, len(afterString)):
	     if ".jsn" not in afterString[i]: continue
	     if "index" in afterString[i]: continue
	     if "EoLS" in afterString[i]: continue
	     if "EoR" in afterString[i]: continue
	     if "TEMP" in afterString[i]: continue
	     if(float(debug) >= 50): log.info("FILE: {0}".format(afterString[i]))
	     inputJsonFile = os.path.join(inputDataFolder, afterString[i])
	     if(float(debug) >= 50): log.info("inputJsonFile: {0}".format(inputJsonFile))

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

             # this is the number of input and output events, and the name of the dat file, something critical
	     # eventsOutput is actually the total number of events to merge in the macromerged stage
             eventsInput       = int(settings['data'][0])
             eventsOutput      = int(settings['data'][1])
             errorCode         = 0
	     file              = ""
             nFilesBU         = 0
             eventsTotalInput = 0
	     if typeMerging == "mini":
                eventsOutputError = int(settings['data'][2])
		errorCode	  = int(settings['data'][3])
		file              = str(settings['data'][4])
	        if(float(debug) >= 50): log.info("Info from json file(eventsInput, eventsOutput, eventsOutputError, errorCode, file): {0}, {1}, {2}, {3}, {4}".format(eventsInput, eventsOutput, eventsOutputError, errorCode, file))
	        # processed events == input + error events
		eventsInput = eventsInput + eventsOutputError
	     else: 
		errorCode	 = int(settings['data'][2])
		file             = str(settings['data'][3])
                nFilesBU         = int(settings['data'][4])
                eventsTotalInput = int(settings['data'][5])

             fileNameString = file.split('_')

             key = (fileNameString[0],fileNameString[1],fileNameString[2])
             if key in filesDict.keys():
	     	filesDict[key].append(file)
	     	jsonsDict[key].append(inputJsonRenameFile)

                errorCode = (errorCodeDict[key][0]|errorCode)
	     	errorCodeDict[key].remove(errorCodeDict[key][0])
                errorCodeDict.update({key:[errorCode]})

		eventsInput = eventsIDict[key][0] + eventsInput
	     	eventsIDict[key].remove(eventsIDict[key][0])
	     	eventsIDict.update({key:[eventsInput]})

		eventsOutput = eventsODict[key][0] + eventsOutput
	     	eventsODict[key].remove(eventsODict[key][0])
	     	eventsODict.update({key:[eventsOutput]})

		nFilesBU = nFilesBUDict[key][0] + nFilesBU
	     	nFilesBUDict[key].remove(nFilesBUDict[key][0])
	     	nFilesBUDict.update({key:[nFilesBU]})

	     else:
                if(float(debug) >= 50): log.info("Adding {0} to filesDict".format(key))
	     	filesDict.update({key:[file]})
	     	jsonsDict.update({key:[inputJsonRenameFile]})
                errorCodeDict.update({key:[errorCode]})

	     	eventsIDict.update({key:[eventsInput]})

	     	eventsODict.update({key:[eventsOutput]})

	     	nFilesBUDict.update({key:[nFilesBU]})

             if(float(debug) >= 50): log.info("filesDict: {0}\njsonsDict: {1}\n, eventsIDict: {2}, eventsODict: {3}, nFilesBUDict: {4}, errorCodeDict: {5}".format(filesDict, jsonsDict, eventsIDict, eventsODict, nFilesBUDict, errorCodeDict))

             theOutputEndName = outputEndName
	     if optionMerging != "optionA":
                theOutputEndName = "StorageManager"
	     
	     if typeMerging == "mini": 
        	keyEoLS = (fileNameString[0],fileNameString[1])
		if keyEoLS not in eventsEoLSDict.keys():
	           EoLSName = path_eol + "/" + fileNameString[0] + "/" + fileNameString[0] + "_" + fileNameString[1] + "_EoLS.jsn"
                   if(float(debug) >= 10): log.info("EoLSName: {0}".format(EoLSName))
                   if os.path.exists(EoLSName):
                      inputEoLSName = open(EoLSName, "r").read()
                      settingsEoLS  = json.loads(inputEoLSName)
                      eventsEoLS    = int(settingsEoLS['data'][0])
                      filesEoLS     = int(settingsEoLS['data'][1])
                      eventsAllEoLS = int(settingsEoLS['data'][2])
		      eventsEoLSDict.update({keyEoLS:[eventsEoLS,filesEoLS,eventsAllEoLS]})

        	if keyEoLS in eventsEoLSDict.keys(): # and key in filesDict.keys():
        	#try:
		   if(float(debug) >= 20): log.info("mini-EventsEoLS/EventsInput-LS/Stream: {0}, {1}, {2}, {3}".format(eventsEoLS, eventsIDict[key][0], fileNameString[1], fileNameString[2]))
                   if(eventsEoLSDict[keyEoLS][0] == eventsIDict[key][0]):
	              # merged files
	              outMergedFile = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_" + theOutputEndName + ".dat";
	              outMergedJSON = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_" +    outputEndName + ".jsn";

                      if nLoops <= nWithPollMax or nWithPollMax < 0:
                         process = thePool.apply_async(         mergeFiles,       [outputMergedFolder, outputSMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, eventsEoLSDict[keyEoLS], eventsODict[key][0], filesDict[key], jsonsDict[key], errorCodeDict[key][0], typeMerging, doRemoveFiles, outputEndName, optionMerging, debug])
		      else:
                         thread.start_new_thread( mergeFiles,                     (outputMergedFolder, outputSMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, eventsEoLSDict[keyEoLS], eventsODict[key][0], filesDict[key], jsonsDict[key], errorCodeDict[key][0], typeMerging, doRemoveFiles, outputEndName, optionMerging, debug) )
                   else:
                      if (float(debug) >= 20):
                	  log.info("Events number does not match: EoL says {0} we have in the files: {1}".format(eventsEoLSDict[keyEoLS][0], eventsIDict[key][0]))
        	else:
        	#except Exception:
                      if (float(debug) >= 20):
                	  log.warning("Looks like {0} is not in filesDict".format(key))

             else:
		if(float(debug) >= 20): log.info("macro-EventsTotalInput/EventsInput-LS/Stream: {0}, {1}, {2}, {3}".format(eventsTotalInput,eventsIDict[key][0],fileNameString[1],fileNameString[2]))
                if(eventsTotalInput > 0 and eventsTotalInput == eventsIDict[key][0]):
	           # merged files
	           outMergedFile = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_" + theOutputEndName + ".dat";
	           outMergedJSON = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_" + theOutputEndName + ".jsn";

        	   keyEoLS = (fileNameString[0],fileNameString[1])
                   eventsEoLS	 = eventsIDict[key][0]
                   filesEoLS	 = nFilesBUDict[key][0]
                   eventsAllEoLS = eventsTotalInput
		   eventsEoLSDict.update({keyEoLS:[eventsEoLS,filesEoLS,eventsAllEoLS]})

                   if nLoops <= nWithPollMax or nWithPollMax < 0:
                      process = thePool.apply_async(         mergeFiles,       [outputMergedFolder, outputSMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, eventsEoLSDict[keyEoLS], eventsODict[key][0], filesDict[key], jsonsDict[key], errorCodeDict[key][0], typeMerging, doRemoveFiles, outputEndName, optionMerging, debug])
                   else:
                         thread.start_new_thread( mergeFiles,                  (outputMergedFolder, outputSMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, eventsEoLSDict[keyEoLS], eventsODict[key][0], filesDict[key], jsonsDict[key], errorCodeDict[key][0], typeMerging, doRemoveFiles, outputEndName, optionMerging, debug) )
                else:
                   if (float(debug) >= 20):
                       log.info("Events number does not match: EoL says {0}, we have in the files: {1}".format(eventsOutput, eventsIDict[key][0]))

          before = after
      if nLoops <= nWithPollMax and nWithPollMax > 0:
         thePool.close()
         thePool.join()


def start_merging(paths_to_watch, path_eol, typeMerging, outputMerge, outputSMMerge, outputEndName, doRemoveFiles, optionMerging, debug):
    if not os.path.exists(outputMerge):
       try:
          os.makedirs(outputMerge)
       except OSError, e:
          log.warning("Looks like the directory {0} has just been created by someone else...".format(outputMerge))
    
    if not os.path.exists(outputSMMerge):
       try:
          os.makedirs(outputSMMerge)
       except OSError, e:
          log.warning("Looks like the directory {0} has just been created by someone else...".format(outputSMMerge))
    
    if typeMerging != "mini" and typeMerging != "macro" and typeMerging != "auto":
       msg = "Wrong type of merging: %s" % typeMerging
       raise RuntimeError, msg
    
    if typeMerging == "auto":
       theHost = socket.gethostname()
       if "bu" in theHost.lower():
          typeMerging = "mini"
       else:
          typeMerging = "macro"
    
    if typeMerging == "mini":
       if not os.path.exists(path_eol):
          msg = "End of Lumi folder Not Found: %s" % path_eol
          raise RuntimeError, msg
    
    doTheRecovering(paths_to_watch, debug)
    doTheMerging(paths_to_watch, path_eol, typeMerging, debug, outputMerge, outputSMMerge, outputEndName, doRemoveFiles, optionMerging)
