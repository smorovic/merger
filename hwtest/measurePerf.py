#!/usr/bin/env python

from configobj import ConfigObj

from optparse import OptionParser
import os, sys
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


if __name__ == '__main__':

    parser = OptionParser(usage="usage: %prog [-h|--help] -f|--file perf -c|--config config")

    parser.add_option("-f", "--file",
                      action="store", dest="perfFile",
                      help="Performance file containing the results of the merge. Absolute path is needed. Mandatory argument")

    parser.add_option("-c", "--config",
                      action="store", dest="confFile",
                      help="Configuration file. Absolute path is needed. Mandatory argument")


    options, args = parser.parse_args()

    if len(args) != 0:
        parser.error("You specified an invalid option - please use -h to review the allowed options")
    
    if (options.perfFile == None):
        parser.error('Please specify the performance file using the -f/--file option')

    if (options.confFile == None):
        parser.error('Please specify the config file using the -c/--config option')

    params = configureStreams(options.confFile)
    streamsNb = params['Streams']['number']
    
    streamSize = {}
    for i in range(int(streamsNb)):
        streamSize.update({params['Streams']['name' + str(i)] : int(params['Streams']['size' + str(i)])})
    startTimes = {}
    endTimes = {}    
    processTimes = {}

    p = open(options.perfFile, 'r')
    for line in p:
        if "TEMP" not in line: continue
        #print line.split(".")[3]
        stream = line.split(".")[3].split("_TEMP")[0]
        #print stream
        ls = line.split(".")[2]
        if "Time for merging" in line:
            endTimes.update({stream + "." + ls : line.split(" :")[0]})
            processTimes.update({stream + "." + ls : float(line.split(": ")[2].strip("\n"))})
        elif "Start merge of" in line:
            startTimes.update({stream + "." + ls : line.split(" :")[0]})

    p.close()    
 
    #strmStat = {streamName: [nbofLumiSections, total processing time]}
    strmStat = {}
    #lumiStat = {lumiSection : [startTime, EndTime, nbofStreams, totalStreamSize]}
    lumiStat = {}
    
    globalStart = "99:99:99"
    globalEnd = "00:00:00"
    globalSize = 0
    for key in endTimes.keys():
        strm = key.split(".")[0]
        ls = key.split(".")[1]
        if ls in lumiStat:
            if endTimes[key] > lumiStat[ls][1]:
                lumiStat[ls][1] = endTimes[key]
            if startTimes[key] < lumiStat[ls][0]:
                lumiStat[ls][0] = startTimes[key]
            lumiStat[ls][2] += 1
            lumiStat[ls][3] += 50*streamSize[strm]
        else:
             lumiStat.update({ls : [startTimes[key], endTimes[key], 1, 50*streamSize[strm]]})
      
        if strm in strmStat:
            strmStat[strm] = [strmStat[strm][0] + 1, strmStat[strm][1] + processTimes[key]]
        else:
            strmStat.update({strm: [1, processTimes[key]]})

        if globalStart > startTimes[key]:
            globalStart = startTimes[key]
        if globalEnd < endTimes[key]:
            globalEnd = endTimes[key]
        globalSize += 50*streamSize[strm]
   

    print "\033[1mStream statistics:\033[0m"
    for key in sorted(strmStat.iterkeys()):
        print "Stream ", key + ", merged 50 files of" ,streamSize[key], "MB each,", strmStat[key][0], "lumi sections, total merging time: %.2f" %strmStat[key][1]
        
    print "\n\033[1mLumi sections statistics:\033[0m"
    for key in sorted(lumiStat.iterkeys()):
        print "Lumi section", key, "consisted of", lumiStat[key][2], " streams of a total of", lumiStat[key][3], "MB merged; merging started at", lumiStat[key][0], "and ended at", lumiStat[key][1]

    print "\n\033[1mGlobal statistics:\033[0m"
    print "Merged a total of", streamsNb, "streams and", len(lumiStat), "lumi sections, meaning a total of", globalSize /1000, "GB, started at", globalStart, "and ended at", globalEnd  

    print "Finished analysis, exiting..."
 
