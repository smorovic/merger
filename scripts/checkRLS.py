#!/usr/bin/env python

import os, time, sys, getopt, fcntl, shutil, json, zlib, glob
import cmsDataFlowMerger

valid = ['input=', "eols=", "file=", "type=", 'iniArea=', 'help']

usage  =  "Usage: checkRLS.py --input=</fff/output>\n"
usage +=  "                   --eols=</fff/ramdisk>\n"
usage +=  "                   --type=<mini>\n"
usage +=  "                   --iniArea=</store/lustre/mergeMacro>\n"
usage +=  "                   --file=<a_b_c>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

inputDataFolder  = "/fff/output"
EoLSDataFolder   = "/fff/ramdisk"
dataString       = "run153_ls0_streamDQM1"
typeMerging      = "mini"
iniArea          = "/store/lustre/mergeMacro"

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--input":
      inputDataFolder = arg
   if opt == "--out":
      outputDataFolder = arg
   if opt == "--eols":
      EoLSDataFolder = arg
   if opt == "--file":
      dataString = arg
   if opt == "--type":
      typeMerging = arg
   if opt == "--iniArea":
      iniArea = arg

if not os.path.exists(inputDataFolder):
   msg = "BIG PROBLEM, inputDataFolder not found!: %s" % (inputDataFolder)
   raise RuntimeError, msg

fileString = dataString.split('_')

inpSubFolder = fileString[2]

inputDataFolder = os.path.join(inputDataFolder, fileString[0], inpSubFolder)
EoLSDataFolder  = os.path.join(EoLSDataFolder, fileString[0])
eventsInput      = 0
eventsInputFiles = 0
eventsTotalInput = 0

EoLSName = EoLSDataFolder + "/" + fileString[0] + "_" + fileString[1] + "_EoLS.jsn"
if(typeMerging == "mini"):
   if not os.path.exists(EoLSName):
      msg = "BIG PROBLEM, EoLSName not found!: %s" % (EoLSName)
      raise RuntimeError, msg

after = dict()
try:
   after_temp = dict ([(f, None) for f in glob.glob(os.path.join(inputDataFolder, '*.jsn'))])
   after.update(after_temp)
except Exception, e:
   log.error("glob.glob operation failed: {0} - {1}".format(inputDataFolder,e))
afterStringNoSorted = [f for f in after if ((dataString in f) and ("EoLS" not in f) and ("BoLS" not in f) and ("EoR" not in f))]
afterString = sorted(afterStringNoSorted, reverse=False)
print afterString

dataFiles = []
for i in range(0, len(afterString)):

   jsonFile = os.path.join(inputDataFolder, afterString[i])
   print jsonFile
   settings = cmsDataFlowMerger.readJsonFile(jsonFile,0)
      
   if  ("bad" in settings):
      print "corrupted file: ",jsonFile
      continue

   eventsInput      = eventsInput + int(settings['data'][0])
   eventsInputFiles = eventsInputFiles + 1

   if(typeMerging != "mini"):
      eventsTotalInput = int(settings['data'][7])

   fileDataString = afterString[i].split('_')
   dataFiles.append(fileDataString[3].split('.jsn')[0])

iniFiles = []
if(typeMerging == "macro"):
   theStoreIniArea = os.path.join(iniArea, fileString[0], inpSubFolder)
   jsnsIni = sorted(glob.glob(os.path.join(theStoreIniArea, '*.ini')))
   for jsn_file in jsnsIni:
      fileIniString = jsn_file.split('_')
      iniFiles.append(fileIniString[3].split('.ini')[0])

   missingBUs = list(set(iniFiles) - set(dataFiles))
   if(len(missingBUs) != 0):
      print "BUs with ini files and no data files: ",missingBUs

filesEoLS     = 0
eventsAllEoLS = 0
NLostEvents   = 0
if(typeMerging == "mini"):
   inputEoLSName = open(EoLSName, "r").read()
   settingsEoLS  = json.loads(inputEoLSName)
   eventsTotalInput = int(settingsEoLS['data'][0])
   filesEoLS        = int(settingsEoLS['data'][1])
   eventsAllEoLS    = int(settingsEoLS['data'][2])
   NLostEvents      = int(settingsEoLS['data'][3])

print "type({0})/string({1})/files({2}): input = {3} - totalInput = {4}".format(typeMerging,dataString,eventsInputFiles,eventsInput,eventsTotalInput)
