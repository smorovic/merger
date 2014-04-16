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

start_time = datetime.datetime.now()

#______________________________________________________________________________
def main():
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

    lumi_length_mean = 20.
    if (options.lumi_length_mean != None):
       lumi_length_mean = float(options.lumi_length_mean)
    
    lumi_length_sigma = 0.001
    if (options.lumi_length_sigma != None):
       lumi_length_sigma = float(options.lumi_length_sigma)

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

    for ls in range(lumiSections):
       processs = []

       # Produce files every lumi_length_mean seconds with a random flutuation
       sleep_time = seconds_to_sleep(ls, lumi_length_mean, lumi_length_sigma)
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

    parser.add_option("-m", "--lumi-length-mean",
                      action="store", dest="lumi_length_mean",
                      help="Mean length of lumi sections in seconds as a float")

    parser.add_option("-s", "--lumi-length-sigma",
                      action="store", dest="lumi_length_sigma",
                      help="Standard deviation of lumi section length " +
                           "distribution as a float")
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
def seconds_to_sleep(ls, lumi_length_mean=20, lumi_length_sigma=0.001):
    mean_offset = ls * lumi_length_mean
    expected_offset = mean_offset + random.gauss(0., lumi_length_sigma)
    actual_offset = total_seconds(datetime.datetime.now() - start_time)
    ret = max(0., expected_offset - actual_offset)
    return ret
## seconds_to_sleep

#______________________________________________________________________________
def total_seconds(tdelta):
    '''
    Returns the total number of seconds of a datetime.timedelta object.
    '''
    return 3600 * 24 * tdelta.days + tdelta.seconds + 1e-6 * tdelta.microseconds
## total_seconds

#______________________________________________________________________________
def startCreateFiles (streamName, contentInputFile, lumiSections, runNumber, theBUNumber, thePath):
          createFiles(streamName, contentInputFile, lumiSections, runNumber, theBUNumber, thePath)


#______________________________________________________________________________
if __name__ == '__main__':
    main()