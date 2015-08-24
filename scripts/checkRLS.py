#!/usr/bin/env python

import os, time, sys, getopt, fcntl, shutil, json, zlib, requests

valid = ['inp=', "eols=", "file=", "type=", 'help']

usage  =  "Usage: checkRLS.py --inp=</fff/output>\n"
usage +=  "                   --eols=</fff/ramdisk>\n"
usage +=  "                   --file=<a_b_c>\n"
usage +=  "                   --type=<mini>\n"

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

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--inp":
      inputDataFolder = arg
   if opt == "--out":
      outputDataFolder = arg
   if opt == "--eols":
      EoLSDataFolder = arg
   if opt == "--file":
      dataString = arg
   if opt == "--type":
      typeMerging = arg

if not os.path.exists(inputDataFolder):
   msg = "BIG PROBLEM, inputDataFolder not found!: %s" % (inputDataFolder)
   raise RuntimeError, msg

fileString = dataString.split('_')

inputDataFolder = os.path.join(inputDataFolder, fileString[0])
EoLSDataFolder  = os.path.join(EoLSDataFolder, fileString[0])
eventsInput      = 0
eventsInputFiles = 0
eventsTotalInput = 0

EoLSName = EoLSDataFolder + "/" + fileString[0] + "_" + fileString[1] + "_EoLS.jsn"
if(typeMerging == "mini"):
   if not os.path.exists(EoLSName):
      msg = "BIG PROBLEM, EoLSName not found!: %s" % (EoLSName)
      raise RuntimeError, msg

after = dict ([(f, None) for f in os.listdir (inputDataFolder)])     
afterStringNoSorted = [f for f in after]
afterString = sorted(afterStringNoSorted, reverse=False)

for i in range(0, len(afterString)):
   if not afterString[i].endswith(".jsn"): continue
   if dataString not in afterString[i]: continue
   if "EoLS" in afterString[i]: continue
   if "BoLS" in afterString[i]: continue
   if "EoR" in afterString[i]: continue

   jsonFile = os.path.join(inputDataFolder, afterString[i])
   settings_textI = open(jsonFile, "r").read()
   settings = json.loads(settings_textI)

   eventsInput      = eventsInput + int(settings['data'][0])
   eventsInputFiles = eventsInputFiles + 1

   if(typeMerging != "mini"):
      eventsTotalInput = int(settings['data'][7])

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
