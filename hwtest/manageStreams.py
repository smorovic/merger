#!/usr/bin/env python
# -*- coding: utf-8 -*-

from configobj import ConfigObj
from makeFiles import createFiles

import os, sys
from optparse import OptionParser
import multiprocessing
import time,datetime
import random
import math


#______________________________________________________________________________
def main():
    print "DEBUG: Starting ..."
    parser = make_option_parser()

    options, args = parser.parse_args()

    if len(args) != 0:
        parser.error("You specified an invalid option - please use -h to review the allowed options")
    
    if (options.configFile == None):
        parser.error('Please specify the configuration file using the -c/--config option')

    theBUNumber = 10
    if (options.BUNumber != None):
       theBUNumber = options.BUNumber

    thePath = ""
    if (options.Path != None):
       thePath = options.Path

    theInputPath = "dummy"
    if (options.inputPath != None):
       theInputPath = options.inputPath

    if not os.path.exists(theInputPath):
       msg = "BIG PROBLEM, file does not exists!: %s" % str(theInputPath)
       raise RuntimeError, msg

    mean_lumi_length = 20.
    if (options.mean_lumi_length != None):
       mean_lumi_length = options.mean_lumi_length
    
    sigma_lumi_length = 0.001
    if (options.sigma_lumi_length != None):
       sigma_lumi_length = options.sigma_lumi_length
    
    params = configureStreams(options.configFile)
    filesNb = params['Streams']['number']
    lumiSections = int(params['Streams']['ls'])
    runNumber = int(params['Streams']['runnumber'])

    contentInputFile = []
    for i in range(int(filesNb)):
        sizePerFile = int(params['Streams']['size' + str(i)])
        fileName = "inputFile_" + str(int(sizePerFile)) + "MB.dat"
        fullFileName = os.path.join(theInputPath, fileName)
        if not os.path.exists(fullFileName):
           msg = "BIG PROBLEM, file does not exists!: %s" % str(fullFileName)
           raise RuntimeError, msg

        with open(fullFileName, 'r') as theInputfile:
           contentInputFile.append(theInputfile.read())
        theInputfile.close()

    print "DEBUG: Looping over lumis ..."
    for ls in range(lumiSections): 
       processs = []
       
       # Produce files every mean_lumi_length seconds with a random flutuation
       sleep_time = seconds_to_wait(mean_lumi_length, sigma_lumi_length)
       print "DEBUG: sleep time:", sleep_time
       time.sleep(sleep_time)
          
       now = datetime.datetime.now()
   
       print now.strftime("%H:%M:%S"), ": writing ls(%d)" % (ls)
       for i in range(int(filesNb)):
          streamName =  params['Streams']['name' + str(i)]
          process = multiprocessing.Process(target = startCreateFiles, args = [streamName, contentInputFile[i], ls, runNumber, theBUNumber, thePath])
          process.start()

       print now.strftime("%H:%M:%S"), ": finished LS", ls, ", exiting..."
       time.sleep(1)
    now = datetime.datetime.now()
    print now.strftime("%H:%M:%S"), ": finished, exiting..."
## main
    
#______________________________________________________________________________
def make_option_parser():
    parser = OptionParser(usage="usage: %prog [-h|--help] -c|--config config -b|--bu BUNumber | -p|--path Path | -i|--inputPath inputPath")

    parser.add_option("-c", "--config",
                      action="store", dest="configFile",
                      help="Configuration file storing the info related to the simulated streams of data. Absolute path is needed")

    parser.add_option("-b", "--bu",
                      action="store", dest="BUNumber",
                      help="BU number")

    parser.add_option("-p", "--path",
                      action="store", dest="Path",
                      help="Path to make")

    parser.add_option("-i", "--inputPath",
                      action="store", dest="inputPath",
                      help="Input path")

    parser.add_option("-m", "--mean-lumi-length",
                      action="store", dest="mean_lumi_length",
                      help="Mean lenght of lumi sections in seconds as a float")

    parser.add_option("-s", "--sigma-lumi-length",
                      action="store", dest="sigma_lumi_length",
                      help=("Standard deviation of lumi section length " +
                            "distribution as a float"))
    return parser
## make_option_parser


#______________________________________________________________________________
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

#______________________________________________________________________________
def seconds_to_wait(mean_lumi_length=20, sigma_lumi_length=0.001):
    return random.lognormvariate(math.log(mean_lumi_length), sigma_lumi_length)
## seconds_to_wait

#______________________________________________________________________________
def startCreateFiles (streamName, contentInputFile, lumiSections, runNumber, theBUNumber, thePath):
          createFiles(streamName, contentInputFile, lumiSections, runNumber, theBUNumber, thePath)


#______________________________________________________________________________
if __name__ == '__main__':
    main()