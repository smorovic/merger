#!/usr/bin/env python

import os, time, sys, getopt, datetime, fcntl
import random
import json
import shutil
import fileinput

"""
Do actual files
"""
def doFiles(RUNNumber, seeds, timeEnd, rate, path_to_make, streamName, contentInputFile, ls = 5, theBUNumber = 1, theTotalBUs = 1):

   NumberOfFilesPerLS = 5

   random.seed(int(seeds))
   theNLoop = 1
   nInput = 1000
   nOutput = 10
   LSNumber = ls

   start = time.time()
   while ((float(timeEnd) < 0.0 or float(time.time() - start) < float(timeEnd)) and (int(LSNumber) == int(ls))):
     time.sleep (float(rate))
     # just in case we need more than one seed
     numberOfSeedsNeeded = 1
     seedsRND = []
     for i in range(0, numberOfSeedsNeeded):
       seedsRND.append(random.randint(0,999999))

     myDir = "%sunmergedDATA/run%d" % (path_to_make,RUNNumber)
     if not os.path.exists(myDir):
        try:
           os.makedirs(myDir)
        except OSError, e:
           print "Looks like the directory " + myDir + " has just been created by someone else..."

     myDir = "%sunmergedMON/run%d" % (path_to_make,RUNNumber)
     if not os.path.exists(myDir):
        try:
           os.makedirs(myDir)
        except OSError, e:
           print "Looks like the directory " + myDir + " has just been created by someone else..."

     if theNLoop == 1:
     	fileIntNameFullPath = "%sunmergedDATA/run%d/run%d_ls0000_%s_BU%d.ini" % (path_to_make,RUNNumber,RUNNumber,streamName,int(theBUNumber))
     	with open(fileIntNameFullPath, 'w') as thefile:
     	   thefile.write('0' * 10)
     	   thefile.write("\n")
     	thefile.close()

     fileOutputNameFullPath = "%sunmergedDATA/run%d/run%d_ls%d_%s_%d.BU%d.dat" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName,seedsRND[0],int(theBUNumber))
     fileOutputName =                              "run%d_ls%d_%s_%d.BU%d.dat" % (                       RUNNumber,LSNumber,streamName,seedsRND[0],int(theBUNumber))

     # making a symbolic link (to aggressive)
     #msg = "ln -s %s %s" %(contentInputFile,fileOutputNameFullPath)
     #os.system(msg)
     # creating/copying the file (deprecated)
     with open(fileOutputNameFullPath, 'w') as thefile:
        thefile.write(contentInputFile)
     thefile.close()

     outMergedJSONFullPath = "%sunmergedDATA/run%d/run%d_ls%d_%s_%d.BU%d.jsn" % (path_to_make,RUNNumber,RUNNumber,LSNumber,streamName,seedsRND[0],int(theBUNumber))
     theMergedJSONfile = open(outMergedJSONFullPath, 'w')
     theMergedJSONfile.write(json.dumps({'data': (nInput, nOutput, 0, 0, fileOutputName)}))
     theMergedJSONfile.close()
     os.chmod(outMergedJSONFullPath, 0666)

     if(theNLoop%NumberOfFilesPerLS == 0):
        fileJSONNameFullPath = "%sunmergedMON/run%d/run%d_ls%d_EoLS.jsn" % (path_to_make,RUNNumber,RUNNumber,LSNumber)
        if not os.path.exists(fileJSONNameFullPath):
           try:
              theFileJSONName = open(fileJSONNameFullPath, 'w')
              theFileJSONName.write(json.dumps({'data': (nInput*int(NumberOfFilesPerLS), nOutput*int(NumberOfFilesPerLS), nInput*int(NumberOfFilesPerLS)*int(theTotalBUs))}))
              theFileJSONName.close()
              os.chmod(fileJSONNameFullPath, 0666)
           except OSError, e:
              print "Looks like the file " + fileJSONNameFullPath + " has just been created by someone else..."
	
	LSNumber += 1

     theNLoop += 1

   return 0

"""
Main
"""

def createFiles(streamName = "StreamA", contentInputFile = "", ls = 10, RUNNumber = 100, theBUNumber = 1, path_to_make = "", theTotalBUs = 1, rate = 0.0, seeds = 999, timeEnd = -1):
   
   now = datetime.datetime.now()

   print now.strftime("%H:%M:%S"), ": writing ls", ls, ", stream: ", streamName
 
   myDir = "%sunmergedDATA" % (path_to_make)
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
   
   doFiles(int(RUNNumber), seeds, timeEnd, rate, path_to_make, streamName, contentInputFile, ls, theBUNumber, theTotalBUs)

   now = datetime.datetime.now()

   print now.strftime("%H:%M:%S"), ": finished ls", ls, ", stream: ", streamName

   return 0
