#!/usr/bin/env python
import os, time, sys, getopt
import shutil
import json
import glob
import multiprocessing
from multiprocessing.pool import ThreadPool
import threading
import datetime
import fileinput
import socket
import filecmp

# trivial examples
# ./cmsDataFlowMerger.py --paths_to_watch="/ramdisk/output/run100588" --path_eol="/ramdisk/" --outputMerge=/beast_storage/mergeBU --outputEndName=$HOSTNAME --typeMerging=mini  --doRemoveFiles --debug=10
# ./cmsDataFlowMerger.py --paths_to_watch="/store/sata31a02v01/mergeBU/run100588"    --outputMerge=/store/sata31a02v01/mergeMacro --outputEndName=$HOSTNAME --typeMerging=macro --doRemoveFiles --debug=10

# program to merge (cat) files given a list

"""
Do actual merging
"""
def mergeFiles(outputMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, filesJSON):
   if(float(debug) >= 10): print "mergeFiles:", outputMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, filesJSON
   
   outMergedFileFullPath = os.path.join(outputMergedFolder, outMergedFile)
   outMergedJSONFullPath = os.path.join(outputMergedFolder, outMergedJSON)
   if(float(debug) >= 10): print 'outMergedFileFullPath: ',outMergedFileFullPath

   initMergingTime = time.time()
   now = datetime.datetime.now()
   if(float(debug) > 0): print now.strftime("%H:%M:%S"), ": Start merge of ", outMergedJSONFullPath

   if os.path.exists(outMergedFileFullPath):
      os.remove(outMergedFileFullPath)
   if os.path.exists(outMergedJSONFullPath):
      os.remove(outMergedJSONFullPath)

   if typeMerging == "macro":
      fileNameString = files[0].split('_')
      iniName = "../" + fileNameString[0] + "_" + fileNameString[2] + "_" + outputEndName + ".ini"
      iniNameFullPath = os.path.join(outputMergedFolder, iniName)
      if os.path.exists(iniNameFullPath):
         filenames = [iniNameFullPath]
         with open(outMergedFileFullPath, 'w') as fout:
            for line in fileinput.FileInput(filenames):
               fout.write(line)
      else:
         print "BIG PROBLEM, ini file not found!: ",iniNameFullPath

   filenames = [inputDataFolder + "/" + word_in_list for word_in_list in files]
   with open(outMergedFileFullPath, 'a') as fout:
      for line in fileinput.FileInput(filenames):
         fout.write(line)
   fout.close()
   os.chmod(outMergedFileFullPath, 0666)

   # input events in that file, all input events, file name, output events in that files, number of merged files
   # only the first three are important
   theMergedJSONfile = open(outMergedJSONFullPath, 'w')
   theMergedJSONfile.write(json.dumps({'data': (infoEoLS[0], eventsO, outMergedFile, infoEoLS[1], infoEoLS[2])}))
   theMergedJSONfile.close()
   os.chmod(outMergedJSONFullPath, 0666)

   # remove already merged files, if wished
   if(doRemoveFiles == True): 
      for nfile in range(0, len(files)):
         if(float(debug) >= 10): print "removing file: ",files[nfile]
   	 inputFileToRemove = os.path.join(inputDataFolder, files[nfile])
   	 os.remove(inputFileToRemove)
      for nfile in range(0, len(filesJSON)):
         if(float(debug) >= 10): print "removing filesJSON: ",filesJSON[nfile]
   	 inputFileToRemove = os.path.join(inputDataFolder, filesJSON[nfile])
   	 os.remove(inputFileToRemove)

   # Last thing to do is to move the data and json files to its final location "merged/runXXXXXX/open/../."
   outMergedFileFullPathStable = outputMergedFolder + "/../" + outMergedFile
   shutil.move(outMergedFileFullPath,outMergedFileFullPathStable)
   outMergedJSONFullPathStable = outputMergedFolder + "/../" + outMergedJSON
   shutil.move(outMergedJSONFullPath,outMergedJSONFullPathStable)

   endMergingTime = time.time() 
   now = datetime.datetime.now()
   if(float(debug) > 0): print now.strftime("%H:%M:%S"), ": Time for merging(%s): %f" % (outMergedJSONFullPath,endMergingTime-initMergingTime)

"""
Do recovering JSON files
"""
def doTheRecovering():
   inputDataFolders = glob.glob(paths_to_watch)
   if(float(debug) >= 10): print "**************recovering JSON files***************"
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
def doTheMerging():
   filesDict      = dict() 
   jsonsDict      = dict()    
   eventsIDict    = dict()
   eventsODict    = dict()
   eventsEoLSDict = dict()
   nFilesBUDict   = dict() 
   if(float(debug) >= 10): print "I will watch:", paths_to_watch
   # Maximum number with pool option
   nWithPollMax = 10
   # Maximum number of threads to be allowed with the pool option
   nThreadsMax  = 50
   # Number of loops
   nLoops = 0
   while 1:
      thePool = ThreadPool(nThreadsMax)
      nLoops = nLoops + 1
      inputDataFolders = glob.glob(paths_to_watch)
      if(float(debug) >= 10): print "***************NEW LOOP************** ",nLoops
      if(float(debug) >= 10): print inputDataFolders
      for nf in range(0, len(inputDataFolders)):
          inputDataFolder = inputDataFolders[nf]
	  # making output folders
	  inputDataFolderString = inputDataFolder.split('/')
	  # if statement to allow ".../" or ... for input folders 
	  if inputDataFolderString[len(inputDataFolderString)-1] == '':
	    outputMergedFolder = os.path.join(outputMerge, inputDataFolderString[len(inputDataFolderString)-2], "open")
          else:
	    outputMergedFolder = os.path.join(outputMerge, inputDataFolderString[len(inputDataFolderString)-1], "open")
	  if not os.path.exists(outputMergedFolder):
             try:
                os.makedirs(outputMergedFolder)
             except OSError, e:
                 print "Looks like the directory " + outputMergedFolder + " has just been created by someone else..."
	  
	  # reading the list of files in the given folder
          before = dict ([(f, None) for f in os.listdir (inputDataFolder)])
          if(float(debug) >= 10): time.sleep (1)
          if(float(debug) >= 10): print "Begin folder iteration"
          after = dict ([(f, None) for f in os.listdir (inputDataFolder)])     
          afterString = [f for f in after]
          added = [f for f in after if not f in before]
          if(float(debug) >= 50): print afterString
          removed = [f for f in before if not f in after]
          if added: 
             if(float(debug) >= 50): print "Added: ", ", ".join (added)
          if removed: 
             if(float(debug) >= 50): print "Removed: ", ", ".join (removed)

	  # loop over ini files, needs to be done first of all
          for i in range(0, len(afterString)):

	     if(afterString[i].endswith(".ini") and "TEMP" not in afterString[i]):
                inputName  = os.path.join(inputDataFolder,afterString[i])
                if is_completed(inputName) == True:
		   # init name: runxxx_streamY_HOST.ini
		   inputNameString = afterString[i].split('_')
                   outputName =  outputMergedFolder + "/../" + inputNameString[0] + "_" + inputNameString[1] + "_" + outputEndName + ".ini"
		   inputNameRename  = inputName.replace(".ini","_TEMP.ini")
                   shutil.move(inputName,inputNameRename)
                   if(float(debug) >= 10): print "iniFile: ",afterString[i]
	           # getting the ini file, just once per stream
		   if not os.path.exists(outputName):
		      try:
		         shutil.copy(inputNameRename,outputName)
		      except OSError, e:
		         print "Looks like the file " + outputName + " has just been created by someone else..."
	           # otherwise, checking if they are identical
	           else:
		      if filecmp.cmp(outputName,inputNameRename) == False:
		         msg = "ini files: %s and %s are different!!!" % (outputName,inputNameRename)
		         raise RuntimeError, msg

                   if(doRemoveFiles == True): 
                      os.remove(inputNameRename)

		else:
		   print "Looks like the file " + inputName + " is being copied by someone else..."

	  # loop over JSON files, which will give the list of files to be merged
	  for i in range(0, len(afterString)):
	     if ".jsn" not in afterString[i]: continue
	     if "index" in afterString[i]: continue
	     if "EoLS" in afterString[i]: continue
	     if "EoR" in afterString[i]: continue
	     if "TEMP" in afterString[i]: continue
	     if(float(debug) >= 50): print "FILE:", afterString[i]
	     inputJsonFile = os.path.join(inputDataFolder, afterString[i])
	     if(float(debug) >= 50): print "inputJsonFile:",inputJsonFile

             # moving the file to avoid issues
	     inputJsonRenameFile = inputJsonFile.replace(".jsn","_TEMP.jsn")
             shutil.move(inputJsonFile,inputJsonRenameFile)

	     try:
                settings_textI = open(inputJsonRenameFile, "r").read()
                if(float(debug) >= 50): print "trying to load: ",inputJsonRenameFile
	        settings = json.loads(settings_textI)
             except ValueError, e:
                print "Looks like the file " + inputJsonRenameFile + " is not available..."
	        try:
	           time.sleep (0.1)
                   settings_textI = open(inputJsonRenameFile, "r").read()
		   settings = json.loads(settings_textI)
                except ValueError, e:
                   print "Looks like the file " + inputJsonRenameFile + " is not available (2nd try)..."
	           time.sleep (1.0)
                   settings_textI = open(inputJsonRenameFile, "r").read()
		   settings = json.loads(settings_textI)

             # this is the number of input and output events, and the name of the dat file, something critical
	     # eventsOutput is actually the total number of events to merge in the macromerged stage
             eventsInput  = int(settings['data'][0])
             eventsOutput = int(settings['data'][1])
             file         = str(settings['data'][2])
	     if(float(debug) >= 50): print 'Info from json file(eventsInput, eventsOutput, file): ',eventsInput,eventsOutput,file
             nFilesBU         = 0
             eventsTotalInput = 0
	     if typeMerging == "macro": 
                nFilesBU         = int(settings['data'][3])
                eventsTotalInput = int(settings['data'][4])

             fileNameString = file.split('_')

             key = (fileNameString[0],fileNameString[1],fileNameString[2])
             if key in filesDict.keys():
	     	filesDict[key].append(file)
	     	jsonsDict[key].append(inputJsonRenameFile)

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
                if(float(debug) >= 50): print "Adding", key, "to filesDict"
	     	filesDict.update({key:[file]})
	     	jsonsDict.update({key:[inputJsonRenameFile]})

	     	eventsIDict.update({key:[eventsInput]})

	     	eventsODict.update({key:[eventsOutput]})

	     	nFilesBUDict.update({key:[nFilesBU]})

             if(float(debug) >= 50): print "filesDict:    ", filesDict
             if(float(debug) >= 50): print "jsonsDict:    ", jsonsDict
             if(float(debug) >= 50): print "eventsIDict:  ", eventsIDict
             if(float(debug) >= 50): print "eventsODict:  ", eventsODict
             if(float(debug) >= 50): print "nFilesBUDict: ", nFilesBUDict

             if typeMerging == "mini": 
        	keyEoLS = (fileNameString[0],fileNameString[1])
		if keyEoLS not in eventsEoLSDict.keys():
	           EoLSName = path_eol + "/" + fileNameString[0] + "/EoLS_" + fileNameString[1].split('ls')[1] + ".jsn"
                   if(float(debug) >= 10): print "EoLSName: ",EoLSName
                   if os.path.exists(EoLSName):
                      inputEoLSName = open(EoLSName, "r").read()
                      settingsEoLS  = json.loads(inputEoLSName)
                      eventsEoLS    = int(settingsEoLS['data'][0])
                      filesEoLS     = int(settingsEoLS['data'][1])
                      eventsAllEoLS = int(settingsEoLS['data'][2])
		      eventsEoLSDict.update({keyEoLS:[eventsEoLS,filesEoLS,eventsAllEoLS]})

        	if keyEoLS in eventsEoLSDict.keys(): # and key in filesDict.keys():
        	#try:
		   if(float(debug) >= 5): print "mini-EventsEoLS/EventsInput-LS/Stream: ",eventsEoLS,eventsIDict[key][0],fileNameString[1],fileNameString[2]
                   if(eventsEoLSDict[keyEoLS][0] == eventsIDict[key][0]):
	              # merged files
	              outMergedFile = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_" + outputEndName + ".dat";
	              outMergedJSON = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_" + outputEndName + ".jsn";

                      if nLoops <= nWithPollMax:
                         process = thePool.apply_async(         mergeFiles,       [outputMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, eventsEoLSDict[keyEoLS], eventsODict[key][0], filesDict[key], jsonsDict[key]])
		      else:
		         process = threading.Thread   (target = mergeFiles,args = (outputMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, eventsEoLSDict[keyEoLS], eventsODict[key][0], filesDict[key], jsonsDict[key]))
                         process.start()
		         #process = multiprocessing.Process(target = mergeFiles, args = [outputMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, eventsEoLSDict[keyEoLS], eventsODict[key][0], filesDict[key], jsonsDict[key]])
                         #process.start()
                   else:
                      if (float(debug) >= 5):
                	  print "Events number does not match: EoL says", eventsEoLSDict[keyEoLS][0], "we have in the files:", eventsIDict[key][0]
        	else:
        	#except Exception:
                      if (float(debug) >= 50):
                	  print "Looks like", key, "is not in filesDict"

             else:
		if(float(debug) >= 5): print "macro-EventsTotalInput/EventsInput-LS/Stream: ",eventsTotalInput,eventsIDict[key][0],fileNameString[1],fileNameString[2]
                if(eventsTotalInput > 0 and eventsTotalInput == eventsIDict[key][0]):
	           # merged files
	           outMergedFile = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_" + outputEndName + ".dat";
	           outMergedJSON = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_" + outputEndName + ".jsn";

        	   keyEoLS = (fileNameString[0],fileNameString[1])
                   eventsEoLS	 = eventsIDict[key][0]
                   filesEoLS	 = nFilesBUDict[key][0]
                   eventsAllEoLS = eventsTotalInput
		   eventsEoLSDict.update({keyEoLS:[eventsEoLS,filesEoLS,eventsAllEoLS]})

                   if nLoops <= nWithPollMax:
                      process = thePool.apply_async(         mergeFiles,       [outputMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, eventsEoLSDict[keyEoLS], eventsODict[key][0], filesDict[key], jsonsDict[key]])
                   else:
                      process = threading.Thread   (target = mergeFiles,args = (outputMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, eventsEoLSDict[keyEoLS], eventsODict[key][0], filesDict[key], jsonsDict[key]))
                      process.start()
                      #process = multiprocessing.Process(target = mergeFiles, args = [outputMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, eventsEoLSDict[keyEoLS], eventsODict[key][0], filesDict[key], jsonsDict[key]])
                      #process.start()
                else:
                   if (float(debug) >= 5):
                       print "Events number does not match: EoL says", eventsOutput, "we have in the files:", eventsIDict[key][0]

          before = after
      if nLoops <= nWithPollMax:
         thePool.close()
         thePool.join()
"""
Main
"""
valid = ['paths_to_watch=', 'path_eol=', 'typeMerging=', 'outputMerge=', 'outputEndName=', 'doRemoveFiles', 'debug=', 'help']

usage =  "Usage: listdir.py --paths_to_watch=<paths_to_watch>\n"
usage += "                  --path_eol=<eoldir>\n"
usage += "                  --outputMerge=<merged>\n"
usage += "                  --outputEndName=<HOST>\n"
usage += "                  --typeMerging=<mini/macro/auto>\n"
usage += "                  --doRemoveFiles (true if added)\n"
usage += "                  --debug=<0(0/1/5/10/50>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

paths_to_watch = "unmerged"
path_eol       = "eoldir"
outputMerge    = "merged"
outputEndName  = "HOST"
typeMerging    = "mini"
debug          = 0
doRemoveFiles  = False

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--paths_to_watch":
      paths_to_watch = arg
   if opt == "--path_eol":
      path_eol = arg
   if opt == "--outputMerge":
      outputMerge = arg
   if opt == "--outputEndName":
      outputEndName = arg
   if opt == "--typeMerging":
      typeMerging = arg
   if opt == "--debug":
      debug = arg
   if opt == "--doRemoveFiles":
      doRemoveFiles = bool(True)

if not os.path.exists(outputMerge):
   try:
      os.makedirs(outputMerge)
   except OSError, e:
      print "Looks like the directory " + outputMerge + " has just been created by someone else..."

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

doTheRecovering()
doTheMerging()
