#!/usr/bin/env python

import os, time, sys, getopt, fcntl, shutil, json, zlib, requests

valid = ['run=', 'stream=', 'help']

usage  = "Usage: recoverFiles.py --run=<run>\n"
usage += "                       --stream=<stream>\n"
usage += "                       --type=<cdaq>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

run = 10000
stream = "Physics"
type = "cdaq"

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--run":
      run = arg
   if opt == "--stream":
      stream = arg
   if opt == "--type":
      type = arg

lsmin = 1
lslimit = 100

eventsInputMax = 0.0
eventsInput = 0.0
eventsOutput = 0.0

for ls in range(lsmin,lslimit):
   if(type == "cdaq"):
      res = requests.post('http://es-cdaq.cms:9200/runindex_cdaq_read/minimerge/_search?pretty&size=1000','{"query":{ "bool":{"must":{"prefix":{"_id":"run'+str(run)+'"}},"must":{"term":{"ls":'+str(ls)+'}},"must":{"term":{"stream":"'+stream+'"}} }}}')
   else:
      res = requests.post('http://es-cdaq.cms:9200/runindex_minidaq_read/minimerge/_search?pretty&size=1000','{"query":{ "bool":{"must":{"prefix":{"_id":"run'+str(run)+'"}},"must":{"term":{"ls":'+str(ls)+'}},"must":{"term":{"stream":"'+stream+'"}} }}}')
   js = json.loads(res.content)

   hitlist = js['hits']['hits']#['_source']['in']
   print "statistics for run",run,"stream",stream,"ls",ls,"  number of BUs reporting mini-merge:",len(hitlist)
   try:
      tot = hitlist[0]['_source']['eolField2']

      totproc=0
      totacc=0

      for hit in hitlist:
	 totproc+=hit['_source']['processed']
	 totacc+=hit['_source']['accepted']
	 totEoL=hit['_source']['eolField2']
	 if totEoL!=tot: print 'mismatch',totEoL,tot

      if tot!=totproc:
	 print "Processed",totproc,"accepted",totacc,"	EoLS total:",tot,"INCOMPLETE!!!"
      else:
	 print "Processed",totproc,"accepted",totacc,"	EoLS total:",tot

      eventsInputMax  = eventsInputMax  + tot
      eventsInput     = eventsInput     + totproc
      eventsOutput    = eventsOutput    + totacc
   except Exception, e:
      print e

print "Total(inputMax/input/output): ",eventsInputMax," ",eventsInput," ",eventsOutput," lost = ",1.0-eventsInput/eventsInputMax
