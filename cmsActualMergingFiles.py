#!/usr/bin/env python
# -*- coding: utf-8 -*-
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

"""
merging option A: merging unmerged files to different files for different BUs
"""
def mergeFilesA(outputMergedFolder, outputDQMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, fileSize, filesJSON, errorCode, typeMerging, doRemoveFiles, outputEndName, outputMonFolder, debug):

   if(float(debug) >= 10): log.info("mergeFiles: {0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}".format(outputMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, fileSize, filesJSON, errorCode))
   
   outMergedFileFullPath = os.path.join(outputMergedFolder, outMergedFile)
   outMergedJSONFullPath = os.path.join(outputMergedFolder, outMergedJSON)
   outMonJSONFullPath    = os.path.join(outputMonFolder,    outMergedJSON)
   if(float(debug) >= 10): log.info('outMergedFileFullPath: {0}'.format(outMergedFileFullPath))

   initMergingTime = time.time()
   now = datetime.datetime.now()
   if(float(debug) > 0): log.info("{0}: Start merge of {1}".format(now.strftime("%H:%M:%S"), outMergedJSONFullPath))

   if os.path.exists(outMergedFileFullPath):
      os.remove(outMergedFileFullPath)
   if os.path.exists(outMergedJSONFullPath):
      os.remove(outMergedJSONFullPath)

   inputJsonFolder = os.path.dirname(filesJSON[0])
   fileNameString = filesJSON[0].replace(inputJsonFolder,"").replace("/","").split('_')

   if (typeMerging == "macro" and fileNameString[2] != "streamDQMhistograms"):
      iniName = "../" + fileNameString[0] + "_ls0000_" + fileNameString[2] + "_" + outputEndName + ".ini"
      iniNameFullPath = os.path.join(outputMergedFolder, iniName)
      if os.path.exists(iniNameFullPath):
         fileSize = os.path.getsize(iniNameFullPath) + fileSize
         filenames = [iniNameFullPath]
         with open(outMergedFileFullPath, 'w') as fout:
            append_files(filenames, fout)
         fout.close()
      else:
         log.error("BIG PROBLEM, ini file not found!: {0}".format(iniNameFullPath))

   filenames = [inputDataFolder + "/" + word_in_list for word_in_list in files]

   if(float(debug) > 5): log.info("Will merge: {0}".format(filenames))

   if (fileNameString[2] != "streamDQMhistograms"):
      with open(outMergedFileFullPath, 'a') as fout:
         append_files(filenames, fout)
      fout.close()
      if(float(debug) > 5): log.info("Merged: {0}".format(filenames))
      #os.chmod(outMergedFileFullPath, 0666)
   
   else:
      msg = "fastHadd add -o %s " % (outMergedFileFullPath)
      for nfile in range(0, len(filenames)):
         msg = msg + filenames[nfile] + " "
      os.system(msg)

   # input events in that file, all input events, file name, output events in that files, number of merged files
   # only the first three are important
   theMergedJSONfile = open(outMergedJSONFullPath, 'w')
   theMergedJSONfile.write(json.dumps({'data': (infoEoLS[0], eventsO, errorCode, outMergedFile, fileSize, infoEoLS[1], infoEoLS[2])}))
   theMergedJSONfile.close()
   #os.chmod(outMergedJSONFullPath, 0666)

   # used for monitoring purposes
   if typeMerging == "mini":
      try:
         shutil.copy(outMergedJSONFullPath,outMonJSONFullPath)
      except OSError, e:
         log.warning("failed copy from {0} to {1}...".format(outMergedJSONFullPath,outMonJSONFullPath))

   # remove already merged files, if wished
   if(doRemoveFiles == "True"):
      for nfile in range(0, len(files)):
         if(float(debug) >= 10): log.info("removing file: {0}".format(files[nfile]))
   	 inputFileToRemove = os.path.join(inputDataFolder, files[nfile])
         if (os.path.exists(inputFileToRemove) and (not os.path.isdir(inputFileToRemove))):
   	    os.remove(inputFileToRemove)
      for nfile in range(0, len(filesJSON)):
         if(float(debug) >= 10): log.info("removing filesJSON: {0}".format(filesJSON[nfile]))
   	 inputFileToRemove = filesJSON[nfile]
         if (os.path.exists(inputFileToRemove) and (not os.path.isdir(inputFileToRemove))):
   	    os.remove(inputFileToRemove)
      if typeMerging == "mini":
         # Removing BoLS file, the last step
         BoLSFileName = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_BoLS.jsn"
         BoLSFileNameFullPath = os.path.join(inputJsonFolder, BoLSFileName)
         if os.path.exists(BoLSFileNameFullPath):
	    os.remove(BoLSFileNameFullPath)
         else:
	    log.error("BIG PROBLEM, BoLSFileNameFullPath {0} does not exist".format(BoLSFileNameFullPath))

   # Last thing to do is to move the data and json files to its final location "merged/runXXXXXX/open/../."
   outMergedFileFullPathStable = outputMergedFolder + "/../" + outMergedFile
   outMergedJSONFullPathStable = outputMergedFolder + "/../" + outMergedJSON
   if (typeMerging == "macro" and ("DQM" in fileNameString[2])):
      outMergedFileFullPathStable = os.path.join(outputDQMMergedFolder, outMergedFile)
      outMergedJSONFullPathStable = os.path.join(outputDQMMergedFolder, outMergedJSON)
   shutil.move(outMergedFileFullPath,outMergedFileFullPathStable)
   shutil.move(outMergedJSONFullPath,outMergedJSONFullPathStable)

   if fileSize != os.path.getsize(outMergedFileFullPathStable) and fileNameString[2] != "streamError":
      log.error("BIG PROBLEM, fileSize != outMergedFileFullPath: {0} --> {1}/{2}".format(outMergedFileFullPathStable,fileSize,os.path.getsize(outMergedFileFullPathStable)))

   endMergingTime = time.time() 
   now = datetime.datetime.now()
   if(float(debug) > 0): log.info("{0}, : Time for merging({1}): {2}".format(now.strftime("%H:%M:%S"), outMergedJSONFullPath, endMergingTime-initMergingTime))

"""
merging option B: merging unmerged files to same file for different BUs locking the merged file
"""
def mergeFilesB(outputMergedFolder, outputSMMergedFolder, outputDQMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, fileSize, filesJSON, errorCode, typeMerging, doRemoveFiles, outputEndName, outputMonFolder, debug):

   if(float(debug) >= 10): log.info("mergeFiles: {0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}".format(outputMergedFolder, outputSMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, fileSize, filesJSON, errorCode))
   
   # we will merge file at the BU level only!
   outMergedFileFullPath = os.path.join(outputSMMergedFolder, outMergedFile)
   outMergedJSONFullPath = os.path.join(outputMergedFolder,   outMergedJSON)
   outMonJSONFullPath    = os.path.join(outputMonFolder,      outMergedJSON)
   if(float(debug) >= 10): log.info('outMergedFileFullPath: {0}'.format(outMergedFileFullPath))

   initMergingTime = time.time()
   now = datetime.datetime.now()
   if(float(debug) > 0): log.info("{0}: Start merge of {1}".format(now.strftime("%H:%M:%S"), outMergedJSONFullPath))

   if os.path.exists(outMergedJSONFullPath):
      os.remove(outMergedJSONFullPath)

   inputJsonFolder = os.path.dirname(filesJSON[0])
   fileNameString = filesJSON[0].replace(inputJsonFolder,"").replace("/","").split('_')

   if typeMerging == "mini":
      iniName = "../" + fileNameString[0] + "_ls0000_" + fileNameString[2] + "_" + outputEndName + ".ini"
      iniNameFullPath = os.path.join(outputSMMergedFolder, iniName)
      if os.path.exists(iniNameFullPath):
         fileSize = os.path.getsize(iniNameFullPath) + fileSize
         if (not os.path.exists(outMergedFileFullPath)):
            with open(outMergedFileFullPath, 'a') as fout:
               fcntl.flock(fout, fcntl.LOCK_EX)
               #os.chmod(outMergedFileFullPath, 0666)
               filenames = [iniNameFullPath]
               append_files(filenames, fout)
               fcntl.flock(fout, fcntl.LOCK_UN)
            fout.close()
      else:
         log.error("BIG PROBLEM, ini file not found!: {0}".format(iniNameFullPath))

      filenames = [inputDataFolder + "/" + word_in_list for word_in_list in files]

      if(float(debug) > 20): log.info("Will merge: {0}".format(filenames))

      # first renaming the files
      for nfile in range(0, len(filesJSON)):
   	 inputFile       = filesJSON[nfile]
   	 inputFileRename = filesJSON[nfile].replace("_TEMPAUX.jsn","_DONE.jsn")
         shutil.move(inputFile,inputFileRename)
	 filesJSON[nfile] = filesJSON[nfile].replace("_TEMPAUX.jsn","_DONE.jsn")

      with open(outMergedFileFullPath, 'a') as fout:
         fcntl.flock(fout, fcntl.LOCK_EX)
         append_files(filenames, fout)
         fcntl.flock(fout, fcntl.LOCK_UN)
      fout.close()

   # input events in that file, all input events, file name, output events in that files, number of merged files
   # only the first three are important
   theMergedJSONfile = open(outMergedJSONFullPath, 'w')
   theMergedJSONfile.write(json.dumps({'data': (infoEoLS[0], eventsO, errorCode, outMergedFile, fileSize, infoEoLS[1], infoEoLS[2])}))
   theMergedJSONfile.close()
   #os.chmod(outMergedJSONFullPath, 0666)

   # used for monitoring purposes
   if typeMerging == "mini":
      try:
         shutil.copy(outMergedJSONFullPath,outMonJSONFullPath)
      except OSError, e:
         log.warning("failed copy from {0} to {1}...".format(outMergedJSONFullPath,outMonJSONFullPath))

   # remove already merged files, if wished
   if(doRemoveFiles == "True"):
      if typeMerging == "mini":
         for nfile in range(0, len(files)):
            if(float(debug) >= 10): log.info("removing file: {0}".format(files[nfile]))
   	    inputFileToRemove = os.path.join(inputDataFolder, files[nfile])
            if (os.path.exists(inputFileToRemove) and (not os.path.isdir(inputFileToRemove))):
   	       os.remove(inputFileToRemove)
      for nfile in range(0, len(filesJSON)):
         if(float(debug) >= 10): log.info("removing filesJSON: {0}".format(filesJSON[nfile]))
   	 inputFileToRemove = filesJSON[nfile]
         if (os.path.exists(inputFileToRemove) and (not os.path.isdir(inputFileToRemove))):
   	    os.remove(inputFileToRemove)
      if typeMerging == "mini":
         # Removing BoLS file, the last step
         BoLSFileName = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_BoLS.jsn"
         BoLSFileNameFullPath = os.path.join(inputJsonFolder, BoLSFileName)
         if os.path.exists(BoLSFileNameFullPath):
	    os.remove(BoLSFileNameFullPath)
         else:
	    log.error("BIG PROBLEM, BoLSFileNameFullPath {0} does not exist".format(BoLSFileNameFullPath))

   # Last thing to do is to move the data and json files to its final location "merged/runXXXXXX/open/../."
   if typeMerging == "macro":
      outMergedFileFullPathStable = outputSMMergedFolder + "/../" + outMergedFile
      if ("DQM" in fileNameString[2]):
         outMergedFileFullPathStable = os.path.join(outputDQMMergedFolder, outMergedFile)
      if(float(debug) >= 10): log.info("outMergedFileFullPath/outMergedFileFullPathStable: {0}, {1}".format(outMergedFileFullPath, outMergedFileFullPathStable))
      shutil.move(outMergedFileFullPath,outMergedFileFullPathStable)

      if fileSize != os.path.getsize(outMergedFileFullPathStable) and fileNameString[2] != "streamError":
         log.error("BIG PROBLEM, fileSize != outMergedFileFullPath: {0} --> {1}/{2}".format(outMergedFileFullPathStable,fileSize,os.path.getsize(outMergedFileFullPathStable)))

   outMergedJSONFullPathStable = outputMergedFolder + "/../" + outMergedJSON
   if (typeMerging == "macro" and ("DQM" in fileNameString[2])):
      outMergedJSONFullPathStable = os.path.join(outputDQMMergedFolder, outMergedJSON)
   shutil.move(outMergedJSONFullPath,outMergedJSONFullPathStable)

   endMergingTime = time.time() 
   now = datetime.datetime.now()
   if(float(debug) > 0): log.info("{0}, : Time for merging({1}): {2}".format(now.strftime("%H:%M:%S"), outMergedJSONFullPath, endMergingTime-initMergingTime))

"""
merging option C: merging unmerged files to same file for different BUs without locking the merged file 
"""
def mergeFilesC(outputMergedFolder, outputSMMergedFolder, outputDQMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, fileSize, filesJSON, errorCode, typeMerging, doRemoveFiles, outputEndName, outputMonFolder, debug):

   if(float(debug) >= 10): log.info("mergeFiles: {0}, {1}, {2}, {3}, {4}, {5}, {6}, {7}, {8}, {9}, {10}".format(outputMergedFolder, outputSMMergedFolder, outMergedFile, outMergedJSON, inputDataFolder, infoEoLS, eventsO, files, fileSize, filesJSON, errorCode))

   # we will merge file at the BU level only!
   outMergedFileFullPath = os.path.join(outputSMMergedFolder, outMergedFile)
   outMergedJSONFullPath = os.path.join(outputMergedFolder,   outMergedJSON)
   outMonJSONFullPath    = os.path.join(outputMonFolder,      outMergedJSON)
   if(float(debug) >= 10): log.info('outMergedFileFullPath: {0}'.format(outMergedFileFullPath))

   initMergingTime = time.time()
   now = datetime.datetime.now()
   if(float(debug) > 0): log.info("{0}: Start merge of {1}".format(now.strftime("%H:%M:%S"), outMergedJSONFullPath))

   if os.path.exists(outMergedJSONFullPath):
      os.remove(outMergedJSONFullPath)

   inputJsonFolder = os.path.dirname(filesJSON[0])
   fileNameString = filesJSON[0].replace(inputJsonFolder,"").replace("/","").split('_')

   lockName = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_" + "StorageManager" + ".lock"
   lockNameFullPath = os.path.join(outputSMMergedFolder, lockName)

   if typeMerging == "mini":
      maxSizeMergedFile = 50 * 1024 * 1024 * 1024
      iniName = "../" + fileNameString[0] + "_ls0000_" + fileNameString[2] + "_" + outputEndName + ".ini"
      iniNameFullPath = os.path.join(outputSMMergedFolder, iniName)
      if os.path.exists(iniNameFullPath):
         fileSize = os.path.getsize(iniNameFullPath) + fileSize
         if (not os.path.exists(outMergedFileFullPath)):
            with open(outMergedFileFullPath, 'w') as fout:
               fcntl.flock(fout, fcntl.LOCK_EX)
               fout.truncate(maxSizeMergedFile)
               fout.seek(0)
               #os.chmod(outMergedFileFullPath, 0666)
               filenames = [iniNameFullPath]
               append_files(filenames, fout)

   	       with open(lockNameFullPath, 'w') as filelock:
   	          fcntl.flock(filelock, fcntl.LOCK_EX)
   	          filelock.write("%d" %(os.path.getsize(iniNameFullPath)))
   	          filelock.flush()
   	          #os.fdatasync(filelock)
		  #os.chmod(lockNameFullPath, 0666)
   	          fcntl.flock(filelock, fcntl.LOCK_UN)
   	       filelock.close()

               fcntl.flock(fout, fcntl.LOCK_UN)
            fout.close()
      else:
         log.error("BIG PROBLEM, ini file not found!: {0}".format(iniNameFullPath))
	 msg = "BIG PROBLEM, ini file not found!: %s" % (iniNameFullPath)
	 raise RuntimeError, msg

      filenames = [inputDataFolder + "/" + word_in_list for word_in_list in files]

      if(float(debug) > 20): log.info("Will merge: {0}".format(filenames))

      # first renaming the files (INTENTIONAL WRONG FOR THE TIME BEING)
      for nfile in range(0, len(filesJSON)):
   	 inputFile       = filesJSON[nfile]
   	 inputFileRename = filesJSON[nfile].replace("_TEMPNO.jsn","_DONE.jsn")
         shutil.move(inputFile,inputFileRename)
	 filesJSON[nfile] = filesJSON[nfile].replace("_TEMPNO.jsn","_DONE.jsn")

      sum = 0
      for nFile in range(0,len(filenames)):
         if os.path.exists(filenames[nFile]) and os.path.isfile(filenames[nFile]):
   	    sum = sum + os.path.getsize(filenames[nFile])

      while not os.path.exists(lockNameFullPath):
         if(float(debug) >= 0): log.info("Waiting for the file to exists: {0}".format(lockNameFullPath))
         time.sleep(1)

      with open(lockNameFullPath, 'r+w') as filelock:
         fcntl.flock(filelock, fcntl.LOCK_EX)
         lockFullString = filelock.readline().split(',')
         ini = int(lockFullString[len(lockFullString)-1])
         filelock.write(",%d" % (ini+sum))
         filelock.flush()
         #os.fdatasync(filelock)
         fcntl.flock(filelock, fcntl.LOCK_UN)
      filelock.close()

      with open(outMergedFileFullPath, 'r+w') as fout:
         fout.seek(ini)
         append_files(filenames, fout)
      fout.close()

   # input events in that file, all input events, file name, output events in that files, number of merged files
   # only the first three are important
   theMergedJSONfile = open(outMergedJSONFullPath, 'w')
   theMergedJSONfile.write(json.dumps({'data': (infoEoLS[0], eventsO, errorCode, outMergedFile, fileSize, infoEoLS[1], infoEoLS[2])}))
   theMergedJSONfile.close()
   #os.chmod(outMergedJSONFullPath, 0666)

   # used for monitoring purposes
   if typeMerging == "mini":
      try:
         shutil.copy(outMergedJSONFullPath,outMonJSONFullPath)
      except OSError, e:
         log.warning("failed copy from {0} to {1}...".format(outMergedJSONFullPath,outMonJSONFullPath))

   # remove already merged files, if wished
   if(doRemoveFiles == "True"):
      if typeMerging == "mini":
         for nfile in range(0, len(files)):
            if(float(debug) >= 10): log.info("removing file: {0}".format(files[nfile]))
   	    inputFileToRemove = os.path.join(inputDataFolder, files[nfile])
            if (os.path.exists(inputFileToRemove) and (not os.path.isdir(inputFileToRemove))):
   	       os.remove(inputFileToRemove)
      for nfile in range(0, len(filesJSON)):
         if(float(debug) >= 10): log.info("removing filesJSON: {0}".format(filesJSON[nfile]))
   	 inputFileToRemove = filesJSON[nfile]
         if (os.path.exists(inputFileToRemove) and (not os.path.isdir(inputFileToRemove))):
   	    os.remove(inputFileToRemove)
      if typeMerging == "mini":
         # Removing BoLS file, the last step
         BoLSFileName = fileNameString[0] + "_" + fileNameString[1] + "_" + fileNameString[2] + "_BoLS.jsn"
         BoLSFileNameFullPath = os.path.join(inputJsonFolder, BoLSFileName)
         if os.path.exists(BoLSFileNameFullPath):
	    os.remove(BoLSFileNameFullPath)
         else:
	    log.error("BIG PROBLEM, BoLSFileNameFullPath {0} does not exist".format(BoLSFileNameFullPath))

   # Last thing to do is to move the data and json files to its final location "merged/runXXXXXX/open/../."
   if typeMerging == "macro":

      if not os.path.exists(lockNameFullPath):
         msg = "lock file %s does not exist!\n" % (lockNameFullPath)
	 raise RuntimeError,msg

      with open(lockNameFullPath, 'r+w') as filelock:
         lockFullString = filelock.readline().split(',')
         totalSize = int(lockFullString[len(lockFullString)-1])
      filelock.close()
      if(doRemoveFiles == "True"):
         if (os.path.exists(lockNameFullPath) and (not os.path.isdir(lockNameFullPath))):
            os.remove(lockNameFullPath)

      with open(outMergedFileFullPath, 'r+w') as fout:
         fout.truncate(totalSize)
      fout.close()

      outMergedFileFullPathStable = outputSMMergedFolder + "/../" + outMergedFile
      if ("DQM" in fileNameString[2]):
         outMergedFileFullPathStable = os.path.join(outputDQMMergedFolder, outMergedFile)
      if(float(debug) >= 10): log.info("outMergedFileFullPath/outMergedFileFullPathStable: {0}, {1}".format(outMergedFileFullPath, outMergedFileFullPathStable))
      shutil.move(outMergedFileFullPath,outMergedFileFullPathStable)

      if fileSize != os.path.getsize(outMergedFileFullPathStable) and fileNameString[2] != "streamError":
         log.error("BIG PROBLEM, fileSize != outMergedFileFullPath: {0} --> {1}/{2}".format(outMergedFileFullPathStable,fileSize,os.path.getsize(outMergedFileFullPathStable)))

   outMergedJSONFullPathStable = outputMergedFolder + "/../" + outMergedJSON
   if (typeMerging == "macro" and ("DQM" in fileNameString[2])):
      outMergedJSONFullPathStable = os.path.join(outputDQMMergedFolder, outMergedJSON)
   shutil.move(outMergedJSONFullPath,outMergedJSONFullPathStable)

   endMergingTime = time.time() 
   now = datetime.datetime.now()
   if(float(debug) > 0): log.info("{0}, : Time for merging({1}): {2}".format(now.strftime("%H:%M:%S"), outMergedJSONFullPath, endMergingTime-initMergingTime))


#______________________________________________________________________________
def append_files(ifnames, ofile):
    '''
    Appends the contents of files given by a list of input file names `ifname'
    to the given output file object `ofile'. Returns None.
    '''
    for ifname in ifnames:
        if (os.path.exists(ifname) and (not os.path.isdir(ifname))):
            with open(ifname) as ifile:
                shutil.copyfileobj(ifile, ofile)
            ifile.close()
# append_files
