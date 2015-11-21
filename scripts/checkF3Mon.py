#!/usr/bin/env python

import os, time, sys, getopt, fcntl, shutil, json, zlib, requests

valid = ['input=', 'type=', 'help']

usage  = "Usage: recoverFiles.py --input=<input>\n"
usage += "                       --type=<cdaq>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

inputDataFolder = "/store/lustre/test/kkk"
type = "cdaq"

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--input":
      inputDataFolder = arg
   if opt == "--type":
      type = arg

if not os.path.exists(inputDataFolder):
   msg = "BIG PROBLEM, inputDataFolder not found!: %s" % (inputDataFolder)
   raise RuntimeError, msg

after = dict ([(f, None) for f in os.listdir (inputDataFolder)])     
afterStringNoSorted = [f for f in after]
afterString = sorted(afterStringNoSorted, reverse=False)

# loop over ini files, needs to be done first of all
for i in range(0, len(afterString)):
   if not afterString[i].endswith(".dat"): continue
   outMergedFileFullPath = os.path.join(inputDataFolder , afterString[i])

   iniName = os.path.basename(afterString[i]).split('_')

   if(len(iniName) < 4): continue

   run = iniName[0].replace("run","")
   lsmin = int(iniName[1].replace("ls",""))
   lslimit = int(iniName[1].replace("ls",""))+1
   stream = iniName[2].replace("stream","")

   eventsInput = 0
   eventsOutput = 0
   fileSize = 0

   for ls in range(lsmin,lslimit):
      if(type == "cdaq"):
         res = requests.post('http://es-cdaq:9200/runindex_cdaq/minimerge/_search?pretty&size=1000','{"query":{ "bool":{"must":{"term":{"_parent":'+str(run)+'}},"must":{"term":{"ls":'+str(ls)+'}},"must":{"term":{"stream":"'+stream+'"}} }}}')
      else:
         res = requests.post('http://es-cdaq:9200/runindex_minidaq/minimerge/_search?pretty&size=1000','{"query":{ "bool":{"must":{"term":{"_parent":'+str(run)+'}},"must":{"term":{"ls":'+str(ls)+'}},"must":{"term":{"stream":"'+stream+'"}} }}}')
      js = json.loads(res.content)

      hitlist = js['hits']['hits']#['_source']['in']
      print "statistics for run",run,"stream",stream,"ls",ls,"  number of BUs reporting mini-merge:",len(hitlist)
      tot = hitlist[0]['_source']['eolField2']

      totproc=0
      totacc=0

      for hit in hitlist:
	 fileSize+=hit['_source']['size']
         totproc+=hit['_source']['processed']
         totacc+=hit['_source']['accepted']
         totEoL=hit['_source']['eolField2']
         if totEoL!=tot: print 'mismatch',totEoL,tot

      if tot!=totproc:
         print "  processed",totproc,"accepted",totacc,"   EoLS total:",tot,"INCOMPLETE!!!"
      else:
         print "  processed",totproc,"accepted",totacc,"   EoLS total:",tot
      print "total size: ",fileSize

      eventsInput  = eventsInput  + totproc
      eventsOutput = eventsOutput + totacc
