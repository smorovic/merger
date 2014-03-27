#!/usr/bin/env python


from configobj import ConfigObj
from makeFiles import createFiles

import os, sys
from optparse import OptionParser
import multiprocessing
import datetime

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

def startCreateFiles (streamName, size, lumiSections, runNumber, theBUNumber, result_queue):
    res = createFiles(streamName, size, lumiSections, theBUNumber, RUNNumber = runNumber)
    result_queue.put(res)

if __name__ == '__main__':

    parser = OptionParser(usage="usage: %prog [-h|--help] -c|--config config -b|--bu BUNumber")

    parser.add_option("-c", "--config",
                      action="store", dest="configFile",
                      help="Configuration file storing the info related to the simulated streams of data. Absolute path is needed")

    parser.add_option("-b", "--bu",
                      action="store", dest="BUNumber",
                      help="BU number")

    options, args = parser.parse_args()

    if len(args) != 0:
        parser.error("You specified an invalid option - please use -h to review the allowed options")
    
    if (options.configFile == None):
        parser.error('Please specify the configuration file using the -c/--config option')

    theBUNumber = options.BUNumber

    params = configureStreams(options.configFile)
    filesNb = params['Streams']['number']
    lumiSections = int(params['Streams']['ls'])
    runNumber = int(params['Streams']['runnumber'])
    
    for ls in range(lumiSections): 
        processs = []
        result_queue = multiprocessing.Queue()

        for i in range(int(filesNb)):
            streamName =  params['Streams']['name' + str(i)]
            size = int(params['Streams']['size' + str(i)])
            process = multiprocessing.Process(target = startCreateFiles, args = [streamName, size, ls, runNumber, theBUNumber, result_queue])
            process.start()
            processs.append(process)

        now = datetime.datetime.now()
        print now.strftime("%H:%M:%S"), ": Started all the file producers for ", ls, ", waiting for them to finish...."

        for process in processs:
            process.join()

        results = []
        while len(results) < int(filesNb):
            result = result_queue.get()
            results.append(result)
        now = datetime.datetime.now()
        print now.strftime("%H:%M:%S"), ": All file producers finished lumi section ", ls, " with results: ", results

        for process in processs: # then kill them all off in case... 
            process.terminate()

    print "Finished, exiting..."
