#!/usr/bin/env python

#Note: this scripts needs cmsenv setup

import filecmp
import getopt
import glob
import json
import re
import os
import subprocess
import sys

buDir="/fff/BU0"
topDir="/store/lustre/mergeMacro_TEST"
#topDir="/store/lustre/transfer"
isBU=False
stream="*"
lsPattern="*"

def createJsnDict(pattern):
    rundir = pattern.split('_',3)[0]
    streamdir = pattern.split('_',3)[2]
    jsnPattern = topDir+"/"+rundir+"/"+streamdir+"/jsns/"+pattern+".jsn"
    jsnFiles = glob.glob(jsnPattern)
    print("Considering "+str(len(jsnFiles))+" JSON files "+jsnPattern)
    jsnDict = {}

    for path in jsnFiles:
        filename = path.rsplit('/',1)[-1]
        (run,lsStr,stream,dummy) = filename.split('_',3)
        if stream == "streamError":
            continue

        ls = int(lsStr[2:])
        jsnDict[stream] = jsnDict.get(stream,{})
        jsnDict[stream][ls] = jsnDict[stream].get(ls,[])
        jsnDict[stream][ls].append(path)

    #print(jsnDict)
    return jsnDict

try:
    opts, args = getopt.getopt(sys.argv[1:],"hr:l:s:b",["help","pattern="])
except getopt.GetoptError:
    print("checkFiles.py -p <pattern>")
    sys.exit(2)
for opt,arg in opts:
    if opt in ('-p','--pattern'):
        pattern=arg
    else:
        print("checkFiles.py -p <pattern>")
        sys.exit()

jsnDict = createJsnDict(pattern)

diagResult = re.compile(
    """.*
------------END--------------
read (?P<count>[0-9]+) events
and (?P<badHeader>[0-9]+) events with bad headers
and (?P<badChksum>[0-9]+) events with bad check sum
and (?P<badCompress>[0-9]+) events with bad uncompress
and (?P<duplicate>[0-9]+) duplicated event Id
.*"""
    ,re.DOTALL)

for stream in jsnDict.keys():
    fileCount=0
    badFiles=0
    sys.stdout.write(stream+' ')
    sys.stdout.flush()
    if isBU:
        masterIniFile = None
        for iniFile in glob.glob(topDir+"/run"+run+"/run"+run+"_ls0000_"+stream+"_*ini"):
            if masterIniFile:
                if not filecmp.cmp(iniFile,masterIniFile,False):
                    print("\nIni file "+iniFile+" is different from "+masterIniFile)
            else:
                masterIniFile = iniFile

    lastLS = None
    for ls in sorted(jsnDict[stream].keys(),key=int):
        sys.stdout.write('.')
        #sys.stdout.write(str(ls)+' ')
        sys.stdout.flush()

        if lastLS is None:
            if lsPattern is "*":
                lastLS = 0
            else:
                lastLS = ls - 1
        if ls != lastLS+1:
            print("\nExpected json files for lumi section "+str(lastLS)+", but found the next for LS "+str(ls))
        lastLS = ls

        inputEvents = 0
        for jsn in jsnDict[stream][ls]:
            fileCount += 1
            fileBad = 0
            fp = open(jsn)
            stat = json.load(fp)
            fp.close()
            #print(stat['data'])
            stem=os.path.splitext(jsn)[0]
            datFile = stem.replace("_TEMP","")+".dat"
	    datFile = datFile.replace("/jsns","/data")
            if isBU:
                jsnDatFile = stat['data'][4]
            else:
                jsnDatFile = stat['data'][3]
            if os.path.basename(datFile) != jsnDatFile:
                print("\nWrong file name "+jsnDatFile+" in json file: "+os.path.basename(jsn))
                fileBad=1
            inputEvents += int(stat['data'][0])
            outputEvents = int(stat['data'][1])

            try:
                if isBU:
                    tmpDatFile="/tmp/"+os.path.basename(datFile)
                    output = subprocess.check_output(
                        "cat "+masterIniFile+" "+datFile+">"+tmpDatFile+
                        ";DiagStreamerFile "+tmpDatFile+
                        ";rm "+tmpDatFile,
                        shell=True,
                        stderr=subprocess.STDOUT)
                else:
                    output = subprocess.check_output(
                        "DiagStreamerFile "+datFile,
                        shell=True,
                        stderr=subprocess.STDOUT)
            except subprocess.CalledProcessError:
                print("\nFailed to execute file check: "+output)

            if output.find("Exception") != -1:
                print("\nFound an exception:\n"+output)
                fileBad=1
            else:
                m = diagResult.match(output)
                if outputEvents != int(m.group('count')):
                    print("\nFound "+m.group('count')+" events in file "+datFile+
                          " while JSON claims "+str(outputEvents)+" events")
                    fileBad=1

            if int(m.group('badHeader')) > 0:
                print("\nFound "+m.group('badHeader')+" events with bad header in "+datFile)
                fileBad=1
            if int(m.group('badChksum')) > 0:
                print("\nFound "+m.group('badChksum')+" events with bad checksum in "+datFile)
                fileBad=1
            if int(m.group('badCompress')) > 0:
                print("\nFound "+m.group('badCompress')+" events with bad compression in "+datFile)
                fileBad=1
            if int(m.group('duplicate')) > 0:
                print("\nFound "+m.group('duplicate')+" duplicated events in "+datFile)
                fileBad=1

            badFiles += fileBad

        if isBU:
            fp = open(buDir+"/ramdisk/run"+run+"/run"+run+"_"+ls+"_EoLS.jsn")
            stat = json.load(fp)
            fp.close()
            if int(stat['data'][0]) != inputEvents:
                print("\nMismatch of input events in "+ls+": found "+str(inputEvents)+" events in JSON files but expected "+stat['data'][0]+" from EoLS file")

    if badFiles > 0:
        print("done: "+str(badFiles)+" of "+str(fileCount)+" files are bad")
    else:
        print("done: all "+str(fileCount)+" files are okay")
