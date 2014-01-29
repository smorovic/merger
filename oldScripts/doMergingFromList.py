#!/usr/bin/env python
import os, time, sys, getopt
import shutil
import json
import glob
import multiprocessing
import datetime
import fileinput
import socket

# program to merge (cat) files given a list

"""
Do actual merging
"""
def mergeFiles(outputMergedFolder, outMergedFile, inputDataFolder, files, inputJsonFile):
   print "mergeFiles:", outMergedFile, inputDataFolder, files, inputJsonFile
   
   outMergedFileFullPath = os.path.join(outputMergedFolder, outMergedFile)

   initMergingTime = time.time()
   now = datetime.datetime.now()
   print now.strftime("%H:%M:%S"), ": Start merge of ", inputJsonFile
   if os.path.exists(outMergedFileFullPath):
      os.remove(outMergedFileFullPath)
   # cat all files in one
   for nfile in range(0, len(files)):
      inputFile = os.path.join(inputDataFolder, files[nfile])
      if not os.path.exists(inputFile):
	 # putting back the JSON file to the system
	 inputJsonRenameFile = inputJsonFile.replace("_TEMP.json",".json")
	 shutil.move(inputJsonFile,inputJsonRenameFile)
         msg = "inputFile File Not Found: %s --> %s (json) %s (merged)" % (inputFile,inputJsonFile,outMergedFileFullPath)
         raise RuntimeError, msg

   filenames = [inputDataFolder + "/" + word_in_list for word_in_list in files]
   with open(outMergedFileFullPath, 'w') as fout:
      for line in fileinput.input(filenames):
   	fout.write(line)

   #for nfile in range(0, len(files)):
   #   inputFile = os.path.join(inputDataFolder, files[nfile])
   #   msg  = "cat %s >> %s;" % (inputFile,outMergedFileFullPath)
   #   if(float(debug) > 0): print msg
   #   os.system(msg)
   #   try:
   #       os.remove(inputFile)
   #   except OSError:
   #       print "I tried to remove", inputFile, ", but somebody else did it before I got a chance to"

   # Last thing to do is to move the file to its final location "merged/runXXXXXX/open/../."
   outMergedFileFullPathStable = outputMergedFolder + "/../" + outMergedFile
   shutil.move(outMergedFileFullPath,outMergedFileFullPathStable)

   # remove already merged files, if wished
   if(doRemoveFiles == True and float(debug) != 99): 
      for nfile in range(0, len(files)):
         inputFile = os.path.join(inputDataFolder, files[nfile])
         os.remove(inputFile)

   endMergingTime = time.time() 
   inputJsonRenameFile = inputJsonFile.replace("_TEMP.json","_MERGED.json")
   # back to the initial state if this is just for testing
   if(float(debug) == 99): inputJsonRenameFile = inputJsonFile.replace("_TEMP.json",".json")
   shutil.move(inputJsonFile,inputJsonRenameFile)
   now = datetime.datetime.now()
   if(debug >= 0): print now.strftime("%H:%M:%S"), ": Time for merging(%s): %f" % (inputJsonFile,endMergingTime-initMergingTime)

"""
Do loops
"""
def doTheMerging():
   # get hostname, important
   theHost = socket.gethostname()
   while 1:
      #print paths_to_watch
      inputDataFolders = glob.glob(paths_to_watch)
      if(float(debug) > 0): print "***************NEW LOOP***************"
      if(float(debug) > 0): print inputDataFolders
      for nf in range(0, len(inputDataFolders)):
          inputDataFolder = inputDataFolders[nf]
	  # making output folders
	  inputDataFolderString = inputDataFolder.split('/')
	  outputMergedFolder = os.path.join(outputMerge, inputDataFolderString[len(inputDataFolderString)-1], "open")
          if not os.path.exists(outputMergedFolder):
             try:
                os.makedirs(outputMergedFolder)
             except OSError, e:
                 print "Looks like the directory " + outputMergedFolder + " has just been created by someone else..."
	  
	  # reading the list of files in the given folder
          before = dict ([(f, None) for f in os.listdir (inputDataFolder)])
          if(float(debug) > 0): time.sleep (1)
          if(float(debug) > 0): print "Begin folder iteration"
          after = dict ([(f, None) for f in os.listdir (inputDataFolder)])     
          afterString = [f for f in after]
          added = [f for f in after if not f in before]
          if(float(debug) > 0): print afterString
          removed = [f for f in before if not f in after]
          if added: 
             if(float(debug) > 0): print "Added: ", ", ".join (added)
          if removed: 
             if(float(debug) > 0): print "Removed: ", ", ".join (removed)

	  # loop over JSON files, which will give the list of files to be merged
	  processs = []
          for i in range(0, len(afterString)):
	     if ".dat" in afterString[i]: continue
	     if ".ini" in afterString[i]: continue
	     if ".eof" in afterString[i]: continue
	     if "STS" in afterString[i]: continue
	     if "TEMP" in afterString[i]: continue
	     if "MERGED" in afterString[i]: continue
	     if ".json" not in afterString[i]: continue
	     if theHost not in afterString[i]: continue
	     if(float(debug) > 0): print "FILE:", afterString[i]
	     inputJsonFile = os.path.join(inputDataFolder, afterString[i])
	     if(float(debug) > 0): print "inputJsonFile:",inputJsonFile
             settings_textI = open(inputJsonFile, "r").read()
             settings = json.loads(settings_textI)

             # moving the file to avoid issues
	     inputJsonRenameFile = inputJsonFile.replace(".json","_TEMP.json")
             shutil.move(inputJsonFile,inputJsonRenameFile)

	     # making a dictionary with dat files only
             filesDict = dict()      
             for i in range(0, len(afterString)):
		if not "dat" in afterString[i]: continue
        	fileNameString = afterString[i].split('_')
        	key = (fileNameString[0],fileNameString[1],fileNameString[2])
        	tempFileName = afterString[i]
        	if key in filesDict.keys():
	           filesDict[key].append(tempFileName)
		else:
	           filesDict.update({key:[tempFileName]})
        	if(float(debug) > 0): print "filesDict: ", filesDict

             # this is the actual list of files, something critical
             files = map(str,settings['data'][len(settings['data'])-1])

             # making comparison between the files from the JSON and from the dictionary made 'by hand'
             fileNameString = afterString[i].split('_')
             key = (fileNameString[0],fileNameString[1],fileNameString[2])
             if key in filesDict.keys():
	        if(float(debug) > 0): print "comparison1: ", filesDict[key]
                filesDictJSON = dict()
                for nfile in range(0, len(files)):
                   if key in filesDictJSON.keys():
	              filesDictJSON[key].append(files[nfile])
	           else:
	              filesDictJSON.update({key:[files[nfile]]})
	        if(float(debug) > 0): print "comparison2: ", filesDictJSON[key]
	     else:
	        print "Oh boy, the key " + key + " does not exist!!!"

             # we don't do anything for now, just a warning message
	     if filesDict[key].sort() != filesDictJSON[key].sort():
	        print "Both JSON files are different for: " + fileNameString
                print filesDict[key]
		print filesDictJSON[key]
	        print "***********"

             theSTSJSONfileName = inputJsonFile.replace(".json","_STS.json")             
             if os.path.exists(theSTSJSONfileName):
                os.remove(theSTSJSONfileName)
	     theSTSJSONfile = open(theSTSJSONfileName, 'w')
	     theSTSJSONfile.write(json.dumps({'filelist': str(files)}, sort_keys=True, indent=4, separators=(',', ': ')))
             theSTSJSONfile.close()	  
  
	     # merged file
	     outMergedFileOldFolder = inputJsonFile.replace("_TEMP.json",".dat").split('/')
	     outMergedFile = outMergedFileOldFolder[len(outMergedFileOldFolder)-1]
	     
             if(float(debug) > 0): print "outMergedFile:", outMergedFile
             process = multiprocessing.Process(target = mergeFiles, args = [outputMergedFolder, outMergedFile, inputDataFolder, files, inputJsonRenameFile])
             process.start()
          #for process in processs: # then kill them all off
          #  process.terminate()
          before = after

"""
Main
"""
valid = ['paths_to_watch=', 'typeMerging=', 'outputMerge=', 'debug=', 'help']

usage =  "Usage: listdir.py --paths_to_watch=<paths_to_watch>\n"
usage += "                  --outputMerge=<merged>\n"
usage += "                  --typeMerging=<macro-no_in_used_for_now>\n"
usage += "                  --debug=<0>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

paths_to_watch = "unmerged"
outputMerge    = "merged"
typeMerging    = "macro"
debug          = 0
doRemoveFiles  = False

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--paths_to_watch":
      paths_to_watch = arg
   if opt == "--outputMerge":
      outputMerge = arg
   if opt == "--typeMerging":
      typeMerging = arg
   if opt == "--debug":
      debug = arg
   if opt == "--doRemoveFiles":
      doRemoveFiles = arg

if not os.path.exists(outputMerge):
   try:
      os.makedirs(outputMerge)
   except OSError, e:
       print "Looks like the directory " + outputMerge + " has just been created by someone else..."

doTheMerging()
