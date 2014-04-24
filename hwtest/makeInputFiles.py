#!/usr/bin/env python

from configobj import ConfigObj
from makeFiles import createFiles

import os, sys
from optparse import OptionParser
import multiprocessing
import time,datetime

def configureStreams(fileName):
    streamsConfigFile = fileName 

    try:
        if os.path.isfile(streamsConfigFile):
            config = ConfigObj(streamsConfigFile)
        else:
            print "Configuration file not found: {0}!".format(streamsConfigFile)
            sys.exit(1)
    except IOError, e:
        print "Unable to open configuration file: {0}!".format(streamsConfigFile)
        sys.exit(1)
    
    return config

def startCreateFiles (streamName, size, lumiSections, runNumber, theBUNumber, theOption, thePath, result_queue):
    res = createFiles(streamName, size, lumiSections, runNumber, theBUNumber, theOption, thePath)
    result_queue.put(res)

if __name__ == '__main__':

   parser = OptionParser(usage="usage: %prog [-h|--help] -p|--path Path")

   parser.add_option("-p", "--path",
   		     action="store", dest="Path",
   		     help="Path to make")

   options, args = parser.parse_args()

   if len(args) != 0:
       parser.error("You specified an invalid option - please use -h to review the allowed options")
   
   thePath = "frozen_storage"
   if (options.Path != None):
      thePath = options.Path

   if not os.path.exists(thePath):
      try:
   	 os.makedirs(thePath)
      except OSError, e:
   	 print "Looks like the directory " + thePath + " could not be created..."

   for sizePerFile in (1,3,5): 
       initWritingTime = time.time()
       now = datetime.datetime.now()
       print now.strftime("%H:%M:%S"), ": Started file for size ",sizePerFile," X10MB..."

       fileName = "inputFile_" + str(int(sizePerFile)*10) + "MB.dat"
       fullFileName = os.path.join(thePath, fileName)

       with open(fullFileName, 'w') as thefile:
   	  thefile.write('0' * 1024 * 1024 * (int(sizePerFile) * 10))
          thefile.write("\n")
          thefile.flush()
       thefile.close()

   print "Finished, exiting..."
