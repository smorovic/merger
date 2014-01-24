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
   # cat all files in one
   for nfile in range(0, len(files)):
      inputFile = os.path.join(inputDataFolder, files[nfile])
      if not os.path.exists(inputFile):
	 # putting back the JSON file to the system
	 inputJsonRenameFile = inputJsonFile.replace("_TEMP.json",".json")
         msg  = "mv %s %s" % (inputJsonFile,inputJsonRenameFile)
         os.system(msg)
         msg = "inputFile File Not Found: %s --> %s (json) %s (merged)" % (inputFile,inputJsonFile,outMergedFile)
         raise RuntimeError, msg

   outMergedFileFullPath = outMergedFile
   filenames = [inputDataFolder + "/" + word_in_list for word_in_list in files]
   with open(outMergedFileFullPath, 'w') as fout:
      for line in fileinput.input(filenames):
        fout.write(line)

   #for nfile in range(0, len(files)):
   #   inputFile = os.path.join(inputDataFolder, files[nfile])
   #   msg  = "cat %s >> %s;" % (inputFile,outMergedFile)
   #   if(float(debug) > 0): print msg
   #   os.system(msg)
   #   try:
   #       os.remove(inputFile)
   #   except OSError:
   #       print "I tried to remove", inputFile, ", but somebody else did it before I got a chance to"

   endMergingTime = time.time() 
   inputJsonRenameFile = inputJsonFile.replace("_TEMP.json","_MERGED.json")
   os.remove(inputJsonFile)
   if(float(debug) > 0): print msg
   os.system(msg)
   now = datetime.datetime.now()
   if(debug >= 0): print now.strftime("%H:%M:%S"), ": Time for merging(%s): %f" % (inputJsonFile,endMergingTime-initMergingTime)

"""
Do loops
"""
def doTheMerging():
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
	     if "MERGED" in afterString[i]: continue
	     if "TEMP" in afterString[i]: continue
	     if(float(debug) > 0): print "FILE:", afterString[i]
	     inputJsonFile = os.path.join(inputMonFolders[nf], afterString[i])
	     if(float(debug) > 0): print "inputJsonFile:",inputJsonFile
             settings_textI = open(inputJsonFile, "r").read()
             settings = json.loads(settings_textI)

             # moving the file to avoid issues
	     inputJsonRenameFile = inputJsonFile.replace(".json","_TEMP.json")
             msg  = "mv %s %s" % (inputJsonFile,inputJsonRenameFile)
             os.system(msg)

	     # merged file
	     outMergedFile = settings['outputName'][0]
             if(float(debug) > 0): print "outMergedFile:", outMergedFile
             process = multiprocessing.Process(target = mergeFiles, args = [outMergedFile, inputDataFolder, settings['filelist'], inputJsonRenameFile])
             process.start()
             #processs.append(process)
	     ###mergeFiles(outMergedFile, inputDataFolder, settings['filelist'], inputJsonFile)
          #for process in processs: # then kill them all off
          #  process.terminate()
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

doTheMerging()
