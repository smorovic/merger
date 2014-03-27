#!/usr/bin/env python
import os, time, sys, getopt
import shutil
import json
import glob
import multiprocessing
import datetime
import fileinput

# program to merge (cat) files given a list

"""
Do actual merging
"""
def mergeFiles(outMergedFile, inputDataFolder, files, inputJsonFile):
   #print "mergeFiles:", outMergedFile, inputDataFolder, files, inputJsonFile
    
   initMergingTime = time.time()
   now = datetime.datetime.now()
   print now.strftime("%H:%M:%S"), ": Start merge of ", inputJsonFile
   if os.path.exists(outMergedFile):
      os.remove(outMergedFile)
   msg  = "touch %s;" % outMergedFile
   if(float(debug) > 0): print msg
   os.system(msg)

   if(debug >= 5): print "List of files to be merged: ",files

   outMergedFileFullPath = outMergedFile
   filenames = [inputDataFolder + "/" + word_in_list for word_in_list in files]
   with open(outMergedFileFullPath, 'w') as fout:
      for line in fileinput.input(filenames):
        fout.write(line)

   # files being deleted
   if doRemoveFiles == "True":
      for nfile in range(0, len(files)):
         inputFile = os.path.join(inputDataFolder, files[nfile])
         #os.remove(inputFile)

   endMergingTime = time.time()

   now = datetime.datetime.now()
   if(debug >= 0): print now.strftime("%H:%M:%S"), ": Time for merging(%s): %f" % (inputJsonFile,endMergingTime-initMergingTime)

"""
Do loops
"""
def doTheMerging():
   filesDict = dict()
   BUsDict   = dict()
   while 1:
      #print paths_to_watch
      inputMonFolders = glob.glob(paths_to_watch)
      if(float(debug) > 0): print "***************NEW LOOP***************"
      if(float(debug) > 0): print inputMonFolders
      for nf in range(0, len(inputMonFolders)):
          inputDataFolder = inputMonFolders[nf].replace("MON","DATA")
          if(float(debug) > 0): print "folders MON/DATA: %s - %s" % (inputDataFolder,inputMonFolders[nf])
          before = dict ([(f, None) for f in os.listdir (inputMonFolders[nf])])
          if(float(debug) > 0): time.sleep (1)
          if(float(debug) > 0): print "Begin folder iteration"
          after = dict ([(f, None) for f in os.listdir (inputMonFolders[nf])])     
          afterString = [f for f in after]
          added = [f for f in after if not f in before]
          if(float(debug) > 0): print afterString
          removed = [f for f in before if not f in after]
          if added: 
             if(float(debug) > 0): print "Added: ", ", ".join (added)
          if removed: 
             if(float(debug) > 0): print "Removed: ", ", ".join (removed)

	  processs = []
          for i in range(0, len(afterString)):
	     if ".jsn" not in afterString[i]: continue
	     if "MERGED" in afterString[i]: continue
	     if "TEMP" in afterString[i]: continue
	     if(float(debug) > 0): print "FILE:", afterString[i]
	     inputJsonFile = os.path.join(inputMonFolders[nf], afterString[i])
	     if(float(debug) > 0): print "inputJsonFile:",inputJsonFile
             settings_textI = open(inputJsonFile, "r").read()
             settings = json.loads(settings_textI)

             inputNameString = afterString[i].split('.')
             key = (inputNameString[1],inputNameString[2],inputNameString[3])

             if key in filesDict.keys():
                fileList = filesDict[key][0]
		for nfile in range(0, len(settings['filelist'])):
		   fileList.append(settings['filelist'][nfile])
	     	filesDict[key].remove(filesDict[key][0])
	     	filesDict.update({key:[fileList]})

		nBUs = BUsDict[key][0] + 1
	     	BUsDict[key].remove(BUsDict[key][0])
	     	BUsDict.update({key:[nBUs]})

	     else:
	     	filesDict.update({key:[settings['filelist']]})
	     	BUsDict.update({key:[1]})

             # remove the file to avoid issues
	     if doRemoveFiles == "True":
	        os.remove(inputJsonFile)
             else:
	        inputJsonRenameFile = inputJsonFile.replace(".jsn","_TEMP.jsn")
                shutil.move(inputJsonFile,inputJsonRenameFile)

	     # merged files if number of read BUs >= number of expected BUs
	     if int(BUsDict[key][0]) >= int(expectedBUs):
	        outMergedFile = settings['outputName'][0]
                if(float(debug) > 0): print "outMergedFile:", outMergedFile
                process = multiprocessing.Process(target = mergeFiles, args = [outMergedFile, inputDataFolder, filesDict[key][0], inputJsonFile])
                process.start()

          before = after

"""
Do recovering JSON files
"""
def doTheRecovering(paths_to_watch):
   inputDataFolders = glob.glob(paths_to_watch)
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
Main
"""
valid = ['paths_to_watch=', 'debug=', 'help', 'expectedBUs=', 'doRemoveFiles=']

usage =  "Usage: listdir.py --paths_to_watch=<paths_to_watch>\n"
usage += "                  --expectedBUs=<1>\n"
usage += "                  --doRemoveFiles=<True>\n"
usage += "                  --debug=<0>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

paths_to_watch = "unmerged"
debug          = 0
expectedBUs    = 1
doRemoveFiles  = "True"

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--paths_to_watch":
      paths_to_watch = arg
   if opt == "--debug":
      debug = arg
   if opt == "--expectedBUs":
      expectedBUs = arg
   if opt == "--doRemoveFiles":
      doRemoveFiles = arg

doTheRecovering(paths_to_watch)
doTheMerging()
