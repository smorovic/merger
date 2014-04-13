#!/usr/bin/env python

import os, time, sys, getopt, datetime, fcntl
import random
import json
import shutil
import fileinput

"""
Do actual files
"""
def doFiles(RUNNumber, seeds, timeEnd, rate, path_to_make, streamName, contentInputFile, ls = 5, theBUNumber = 1):

   NumberOfFilesPerLS = 3

   random.seed(int(seeds))
   theNLoop = 1
   nInput = 0
   nOutput = 0
   LSNumber = ls

   start = time.time()
   while ((float(timeEnd) < 0.0 or float(time.time() - start) < float(timeEnd)) and (int(LSNumber) == int(ls))):
     time.sleep (float(rate))
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
     
     fileOutputName = "%sunmergedDATA/Run%d/Data.%d.LS%d.%s.%d.BU%d.raw" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName,seedsRND[0],int(theBUNumber))
     # creating/copying the file
     with open(fileOutputName, 'w') as thefile:
        thefile.write(contentInputFile)
     thefile.close()

     fileNameNoDir =  "Data.%d.LS%d.%s.%d.BU%d.raw" % (int(RUNNumber),int(LSNumber),streamName,seedsRND[0],int(theBUNumber))
     fileNameAUX =  "%sunmergedAUX/Run%d/Data.%d.LS%d.%s.BU%d.dat" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName,int(theBUNumber))
     with open(fileNameAUX, 'a') as thefile:
        thefile.write(fileNameNoDir + "\n")
     thefile.close()

     if(theNLoop%NumberOfFilesPerLS == 0):
        fileJSONName=  "%sunmergedMON/Data.%d.LS%d.%s.BU%d.jsn" % (path_to_make,int(RUNNumber),int(LSNumber),streamName,int(theBUNumber))
        fileNameAUX =  "%sunmergedAUX/Run%d/Data.%d.LS%d.%s.BU%d.dat" % (path_to_make,int(RUNNumber),int(RUNNumber),int(LSNumber),streamName,int(theBUNumber))
        fileAUX = open(fileNameAUX, 'r')
        nFileLine = fileAUX.readline()
	listOfFilesDict = []
	while(nFileLine != ''):
	   nFile = nFileLine.split('\n')
	   listOfFilesDict.append(nFile[0])
           nFileLine = fileAUX.readline()
        fileAUX.close()	   
        #outputName = "%smerged/Run%d/Data.%d.LS%d.%s.BU%d.raw" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName,int(theBUNumber))
        outputName = "%smerged/Run%d/Data.%d.LS%d.%s.raw" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName)
	outputNameDict = [outputName]
        theJSONfile = open(fileJSONName, 'w')
	#theJSONfile.write(json.dumps({'filelist': str(listOfFilesDict), 'outputName': str(outputNameDict)}, sort_keys=True, indent=4, separators=(',', ': ')))
	theJSONfile.write("{\n\"filelist\": [")
        for i in range(0, len(listOfFilesDict)):
	   theJSONfile.write("\"" + listOfFilesDict[i] +"\"")
	   if(i != len(listOfFilesDict)-1): 
	      theJSONfile.write(",")
	theJSONfile.write("],\n\"outputName\": [\"" + outputNameDict[0] +"\"]\n}\n")
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
	
        fileJSONNameFinal =  "%sunmergedMON/Run%d/Data.%d.LS%d.%s.BU%d.jsn" % (path_to_make,int(RUNNumber),int(RUNNumber),int(LSNumber),streamName,int(theBUNumber))
        shutil.move(fileJSONName,fileJSONNameFinal)
	
	nInput = 0
	nOutput = 0
	LSNumber += 1

     if(theNLoop%1000 == 0):
	nInput = 0
	nOutput = 0
	LSNumber += 1
	RUNNumber += 1

     theNLoop += 1

   return 0

"""
Main
"""

#def createFiles(streamName = "StreamA", fullFileName = "", ls = 10, RUNNumber = 100, theBUNumber = 1, path_to_make = "", rate = 0.0, seeds = 999, timeEnd = -1):
def createFiles(streamName = "StreamA", contentInputFile = "", ls = 10, RUNNumber = 100, theBUNumber = 1, path_to_make = "", rate = 0.0, seeds = 999, timeEnd = -1):
   
   now = datetime.datetime.now()

   print now.strftime("%H:%M:%S"), ": writing ls", ls, ", stream: ", streamName
 
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
   
   doFiles(int(RUNNumber), seeds, timeEnd, rate, path_to_make, streamName, contentInputFile, ls, theBUNumber)

   now = datetime.datetime.now()

   print now.strftime("%H:%M:%S"), ": finished ls", ls, ", stream: ", streamName

   return 0
