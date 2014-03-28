#!/usr/bin/env python

import os, time, sys, getopt, datetime, fcntl
import random
import json
import shutil
import fileinput

"""
Do actual files
"""
def doFiles(RUNNumber, seeds, timeEnd, rate, path_to_make, streamName, sizePerFile, ls = 5, theBUNumber = 1, theOption = 0):
   # don't want to touch this at this point
   lumiTimeSlot = 1

   random.seed(int(seeds))
   theNLoop = 1
   nInput = 0
   nOutput = 0
   LSNumber = ls

   start = time.time()
   while ((float(timeEnd) < 0.0 or float(time.time() - start) < float(timeEnd)) and (int(LSNumber) == int(ls))):
     time.sleep (float(rate))
     #print LSNumber, ls
     # just in case we need more than one seed
     numberOfSeedsNeeded = 1
     seedsRND = []
     for i in range(0, numberOfSeedsNeeded):
       seedsRND.append(random.randint(0,999999))

     myDir = "%sunmergedDATA/Run%d" % (path_to_make,RUNNumber)
     if not os.path.exists(myDir):
        try:
           os.makedirs(myDir)
        except OSError, e:
           print "Looks like the directory " + myDir + " has just been created by someone else..."
     myDir = "%sunmergedMON/Run%d" % (path_to_make,RUNNumber)
     if not os.path.exists(myDir):
        try:
           os.makedirs(myDir)
        except OSError, e:
           print "Looks like the directory " + myDir + " has just been created by someone else..."
     myDir = "%sunmergedAUX/Run%d" % (path_to_make,RUNNumber)
     if not os.path.exists(myDir):
        try:
           os.makedirs(myDir)
        except OSError, e:
           print "Looks like the directory " + myDir + " has just been created by someone else..."

     nInput += 100
     nOutput += 9
     
     if int(theOption) == 0:
        fileName = "%sunmergedDATA/Run%d/Data.%d.LS%d.%s.%d.BU%d.raw" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName,seedsRND[0],int(theBUNumber))
        with open(fileName, 'w') as thefile:
           thefile.write('0' * 1024 * 1024 * int(sizePerFile))
           #thefile.write("\n")
	   thefile.flush()
        thefile.close()

     elif int(theOption) == 1:
        fileName = "%sunmergedDATA/Run%d/Data.%d.LS%d.%s.raw" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName)
        with open(fileName, 'a') as thefile:
           fcntl.flock(thefile, fcntl.LOCK_EX)
           thefile.write('0' * 1024 * 1024 * int(sizePerFile))
           #thefile.write("\n")
	   thefile.flush()
           fcntl.flock(thefile, fcntl.LOCK_UN)
        thefile.close()

     elif int(theOption) == 2:
        maxSizeMergedFile = 50 * 1024 * 1024 * 1024
        fileName = "%sunmergedDATA/Run%d/Data.%d.LS%d.%s.raw" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName)
	lockName = "%sunmergedDATA/Run%d/Data.%d.LS%d.%s.lock" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName)
        if not os.path.exists(fileName):
           with open(fileName, 'w') as fout:
              fcntl.flock(fout, fcntl.LOCK_EX)
              fout.truncate(maxSizeMergedFile)
              fout.seek(0)

   	      with open(lockName, 'w') as filelock:
   		 fcntl.flock(filelock, fcntl.LOCK_EX)
   		 filelock.write("%d" %(0))
   		 filelock.flush()
  		 fcntl.flock(filelock, fcntl.LOCK_UN)
   	      filelock.close()

              fcntl.flock(fout, fcntl.LOCK_UN)
           fout.close()

        sum = 1024 * 1024 * int(sizePerFile)
        while not os.path.exists(lockName):
           time.sleep(1)

        with open(lockName, 'r+w') as filelock:
           fcntl.flock(filelock, fcntl.LOCK_EX)
           lockFullString = filelock.readline().split(',')
           ini = int(lockFullString[len(lockFullString)-1])
           filelock.write(",%d" % (ini+sum))
           filelock.flush()
           fcntl.flock(filelock, fcntl.LOCK_UN)
        filelock.close()
        with open(fileName, 'r+w') as fout:
           fout.seek(ini)
	   filenames = [fileName]
           fout.write('0' * 1024 * 1024 * int(sizePerFile))
           #fout.write("\n")
           fout.flush()
        fout.close()

     else:
	msg = "BIG PROBLEM, wrong option!: %d" % int(theOption)
	raise RuntimeError, msg

     fileNameNoDir =  "Data.%d.LS%d.%s.%d.BU%d.raw" % (RUNNumber,LSNumber,streamName,seedsRND[0],int(theBUNumber))
     fileNameAUX =  "%sunmergedAUX/Run%d/Data.%d.LS%d.%s.BU%d.dat" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName,int(theBUNumber))
     msg = "echo %s >> %s" % (fileNameNoDir,fileNameAUX)
     os.system(msg)

     if(theNLoop%50 == 0):
        fileJSONName=  "%sunmergedMON/Data.%d.LS%d.%s.BU%d.jsn" % (path_to_make,RUNNumber,LSNumber,streamName,int(theBUNumber))
        fileNameAUX =  "%sunmergedAUX/Run%d/Data.%d.LS%d.%s.BU%d.dat" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName,int(theBUNumber))
        fileAUX = open(fileNameAUX, 'r')
        nFileLine = fileAUX.readline()
	listOfFilesDict = []
	while(nFileLine != ''):
	   nFile = nFileLine.split('\n')
	   listOfFilesDict.append(nFile[0])
           nFileLine = fileAUX.readline()
        fileAUX.close()	   
        outputName = "%smerged/Run%d/Data.%d.LS%d.%s.BU%d.raw" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName,int(theBUNumber))
	outputNameDict = [outputName]
        theJSONfile = open(fileJSONName, 'w')
	#theJSONfile.write(json.dumps({'filelist': str(listOfFilesDict), 'outputName': str(outputNameDict)}, sort_keys=True, indent=4, separators=(',', ': ')))
	theJSONfile.write("{\n\"filelist\": [")
        for i in range(0, len(listOfFilesDict)):
	   theJSONfile.write("\"")
	   theJSONfile.write(listOfFilesDict[i])
	   theJSONfile.write("\"")
	   if(i != len(listOfFilesDict)-1): 
	      theJSONfile.write(",")
	theJSONfile.write("],\n\"outputName\": [")
	theJSONfile.write("\"")
	theJSONfile.write(outputNameDict[0])
	theJSONfile.write("\"]")
	theJSONfile.write("\n}\n")
        theJSONfile.close()	  
        myDir = "%smerged" % (path_to_make)
        if not os.path.exists(myDir):
           try:
              os.makedirs(myDir)
           except OSError, e:
              print "Looks like the directory " + myDir + " has just been created by someone else..."
        myDir = "%smerged/Run%d" % (path_to_make,RUNNumber)
        if not os.path.exists(myDir):
           try:
              os.makedirs(myDir)
           except OSError, e:
              print "Looks like the directory " + myDir + " has just been created by someone else..."
        os.remove(fileNameAUX)
	
        fileJSONNameFinal =  "%sunmergedMON/Run%d/Data.%d.LS%d.%s.BU%d.jsn" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName,int(theBUNumber))
        shutil.move(fileJSONName,fileJSONNameFinal)
	
	# every lumi section is Xsec
	time.sleep(float(lumiTimeSlot))

	nInput = 0
	nOutput = 0
	LSNumber += 1

     if(theNLoop%1000 == 0):
	nInput = 0
	nOutput = 0
	LSNumber += 1
	RUNNumber += 1

     theNLoop += 1
   #print "Thread finished for stream " + streamName + ", LS " + str(LSNumber)
   return 0

"""
Main
"""

def createFiles(streamName = "StreamA", sizePerFile = 50, ls = 10, RUNNumber = 100, theBUNumber = 1, theOption = 0, path_to_make = "", rate = 0.0, seeds = 999, timeEnd = -1):
    
   myDir = "%sunmergedDATA" % (path_to_make)
   if not os.path.exists(myDir):
      try:
          os.makedirs(myDir)
      except OSError, e:
          print "Looks like the directory " + myDir + " has just been created by someone else..."
   myDir = "%sunmergedAUX" % (path_to_make)
   if not os.path.exists(myDir):
      try:
          os.makedirs(myDir)
      except OSError, e:
          print "Looks like the directory " + myDir + " has just been created by someone else..."

   myDir = "%sunmergedMON" % (path_to_make)
   if not os.path.exists(myDir):
      try:
          os.makedirs(myDir)
      except OSError, e:
          print "Looks like the directory has just been created by someone else..." 
    
   doFiles(int(RUNNumber), seeds, timeEnd, rate, path_to_make, streamName, sizePerFile, ls, theBUNumber, theOption)
   return 0
