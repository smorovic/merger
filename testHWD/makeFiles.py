#!/usr/bin/env python

import os, time, sys, getopt, datetime
import random
import json
import shutil

"""
Do actual files
"""
def doFiles(RUNNumber, seeds, timeEnd, rate, path_to_make, streamName, sizePerFile, ls = 5):
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

     myDir = "%sDATA/Run%d" % (path_to_make,RUNNumber)
     if not os.path.exists(myDir):
     	os.mkdir(myDir)
     myDir = "%sMON/Run%d" % (path_to_make,RUNNumber)
     if not os.path.exists(myDir):
     	os.mkdir(myDir)
     myDir = "%sAUX/Run%d" % (path_to_make,RUNNumber)
     if not os.path.exists(myDir):
     	os.mkdir(myDir)

     nInput += 100
     nOutput += 9
     fileName =  "%sDATA/Run%d/Data.%d.LS%d.%s.%d.raw" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName,seedsRND[0])
     thefile = open(fileName, 'w')
     thefile.write('0' * 1024 * 1024 * int(sizePerFile))
     thefile.write("\n")
     thefile.close()

     fileNameNoDir =  "Data.%d.LS%d.%s.%d.raw" % (RUNNumber,LSNumber,streamName,seedsRND[0])
     fileNameAUX =  "%sAUX/Run%d/Data.%d.LS%d.%s.dat" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName)
     msg = "echo %s >> %s" % (fileNameNoDir,fileNameAUX)
     os.system(msg)

     if(theNLoop%50 == 0):
        fileJSONName=  "%sMON/Data.%d.LS%d.%s.json" % (path_to_make,RUNNumber,LSNumber,streamName)
        fileNameAUX =  "%sAUX/Run%d/Data.%d.LS%d.%s.dat" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName)
        fileAUX = open(fileNameAUX, 'r')
        nFileLine = fileAUX.readline()
	listOfFilesDict = []
	while(nFileLine != ''):
	   nFile = nFileLine.split('\n')
	   listOfFilesDict.append(nFile[0])
           nFileLine = fileAUX.readline()
        fileAUX.close()	   
        outputName = "merged/Run%d/Data.%d.LS%d.%s.raw" % (RUNNumber,RUNNumber,LSNumber,streamName)
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
        myDir = "merged"
        if not os.path.exists(myDir):
           os.mkdir(myDir)
        myDir = "merged/Run%d" % (RUNNumber)
        if not os.path.exists(myDir):
           os.mkdir(myDir)
        os.remove(fileNameAUX)
	
        fileJSONNameFinal =  "%sMON/Run%d/Data.%d.LS%d.%s.json" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName)
	msg = "mv %s %s" % (fileJSONName,fileJSONNameFinal)
        os.system(msg)
	
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

def createFiles(streamName = "StreamA", sizePerFile = 50, ls = 10, path_to_make = "unmerged", rate = 0.0, seeds = 999, timeEnd = -1, RUNNumber = 100):
    
   myDir = "%sDATA" % (path_to_make)
   if not os.path.exists(myDir):
      try:
          os.mkdir(myDir)
      except OSError, e:
          print "Looks like the directory " + myDir + " has just been created by someone else..."
   myDir = "%sAUX" % (path_to_make)
   if not os.path.exists(myDir):
      try:
          os.mkdir(myDir)
      except OSError, e:
          print "Looks like the directory " + myDir + " has just been created by someone else..."

   myDir = "%sMON" % (path_to_make)
   if not os.path.exists(myDir):
      try:
          os.mkdir(myDir)
      except OSError, e:
          print "Looks like the directory has just been created by someone else..." 
    
   doFiles(int(RUNNumber), seeds, timeEnd, rate, path_to_make, streamName, sizePerFile, ls)
   #print "create Files returning for " + streamName
   return 0
