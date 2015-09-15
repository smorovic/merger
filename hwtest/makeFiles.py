#!/usr/bin/env python

import os, time, sys, getopt, datetime, fcntl
import random
import json
import shutil
import fileinput
import zlib

"""
Do actual files
"""
def doFiles(RUNNumber, timeEnd, rate, path_to_make, streamName, contentInputFile, ls = 5, theBUNumber = "AAA", theTotalBUs = 1, numberOfFilesPerLS = 10, nInput = 1000, nOutput = 10):

   theNLoop = 1
   LSNumber = ls

   start = time.time()
   while ((float(timeEnd) < 0.0 or float(time.time() - start) < float(timeEnd)) and (int(LSNumber) == int(ls))):
      time.sleep (float(rate))

      if theNLoop == 1:
         fileBoLSFullPath = "%sunmergedDATA/run%d/%s/run%d_ls%d_%s_BoLS.jsn" % (path_to_make,RUNNumber,streamName,RUNNumber,LSNumber,streamName)
	 msg = "touch %s" % fileBoLSFullPath
	 os.system(msg)

      fileOutputNameFullPath = "%sunmergedDATA/run%d/%s/run%d_ls%d_%s_%d.BU%s.dat" % (path_to_make,RUNNumber,streamName,RUNNumber,LSNumber,streamName,theNLoop,theBUNumber)
      fileOutputName =                                 "run%d_ls%d_%s_%d.BU%s.dat" % (                                  RUNNumber,LSNumber,streamName,theNLoop,theBUNumber)

      # making a symbolic link (sysadmins don't like it)
      #msg = "ln -s %s %s" %(contentInputFile,fileOutputNameFullPath)
      #os.system(msg)
      # creating/copying the file (default)
      with open(fileOutputNameFullPath, 'w') as thefile:
     	 thefile.write(contentInputFile)
      thefile.close()

      fileSize = os.path.getsize(fileOutputNameFullPath)
      adler32c=1
      #calculate checksum on the fly
      with open(fileOutputNameFullPath, 'r') as fsrc:
         length=16*1024
         while 1:
            buf = fsrc.read(length)
            if not buf:
               break
            adler32c=zlib.adler32(buf,adler32c)
      # need to make it unsigned
      adler32c = adler32c & 0xffffffff

      emptyString = ""
      transferDest = "Tier0"
      outMergedJSONFullPath = "%sunmergedDATA/run%d/%s/run%d_ls%d_%s_%d.BU%s.jsn" % (path_to_make,RUNNumber,streamName,RUNNumber,LSNumber,streamName,theNLoop,theBUNumber)
      with  open(outMergedJSONFullPath, 'w') as theMergedJSONfile:
         theMergedJSONfile.write(json.dumps({'data': (nInput, nOutput, 0, 0, fileOutputName, fileSize, emptyString, adler32c, transferDest)}))
      theMergedJSONfile.close()
      ###os.chmod(outMergedJSONFullPath, 0666)

      if(theNLoop%numberOfFilesPerLS == 0):
         LSNumber += 1

      theNLoop += 1

   return 0

"""
Main
"""

def createFiles(streamName = "streamA", contentInputFile = "", ls = 10, RUNNumber = 100, theBUNumber = "AAA", path_to_make = "", theTotalBUs = 1, rate = 0.0, timeEnd = -1, theNumberOfFilesPerLS = 10, theNInput = 1000, theNOutput = 10):
   
   now = datetime.datetime.now()

   print now.strftime("%H:%M:%S"), ": writing ls", ls, ", stream: ", streamName
 
   myDir = "%sunmergedDATA/run%d/%s" % (path_to_make,RUNNumber,streamName)
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
          print "Looks like the directory " + myDir + " has just been created by someone else..." 
   
   doFiles(int(RUNNumber), timeEnd, rate, path_to_make, streamName, contentInputFile, ls, theBUNumber, theTotalBUs, theNumberOfFilesPerLS, theNInput, theNOutput)

   now = datetime.datetime.now()

   print now.strftime("%H:%M:%S"), ": finished ls", ls, ", stream: ", streamName

   return 0
