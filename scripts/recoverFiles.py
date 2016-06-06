#!/usr/bin/env python

import os, time, sys, getopt, fcntl, shutil, json, zlib, requests

def append_files(ifnames, ofile):
    '''
    Appends the contents of files given by a list of input file names `ifname'
    to the given output file object `ofile'. Returns None.
    '''
    for ifname in ifnames:
        if (os.path.exists(ifname) and (not os.path.isdir(ifname))):
            with open(ifname) as ifile:
                shutil.copyfileobj(ifile, ofile)
            ifile.close()

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
   iniNameFullPath = inputDataFolder + "/" + iniName[0] + "_ls0000_"  + iniName[2] + "_StorageManager.ini" 

   outMergedFileFullPath = os.path.join(inputDataFolder , outMergedFileFullPath)
   lockNameFullPath      = outMergedFileFullPath.replace(".dat",".lock")
   outMergedJSONFullPath = outMergedFileFullPath.replace(".dat",".jsn")
   outMergedJSONFullPath = outMergedJSONFullPath.replace("/data","/jsns")

   if not os.path.exists(iniNameFullPath):
      msg = "BIG PROBLEM, ini file not found!: %s" % (iniNameFullPath)
      raise RuntimeError, msg

   with open(outMergedFileFullPath, 'r+w') as fout:
      fout.seek(0)
      filenameIni = [iniNameFullPath]
      append_files(filenameIni, fout)
   fout.close()

   totalSize = 0
   with open(lockNameFullPath, 'r+w') as filelock:
      lockFullString = filelock.readline().split(',')
      totalSize = int(lockFullString[len(lockFullString)-1].split(':')[0].split('=')[1])
   filelock.close()

   with open(outMergedFileFullPath, 'r+w') as fout:
      fout.truncate(totalSize)
   fout.close()

   checkSumFile=1
   with open(outMergedFileFullPath, 'r') as fsrc:
      length=16*1024
      while 1:
         buf = fsrc.read(length)
         if not buf:
	    break
         checkSumFile=zlib.adler32(buf,checkSumFile)

   checkSumFile = checkSumFile & 0xffffffff

   run = iniName[0].replace("run","")
   lsmin = int(iniName[1].replace("ls",""))
   lslimit = int(iniName[1].replace("ls",""))+1
   stream = iniName[2].replace("stream","")

   eventsInput = 0
   eventsOutput = 0

   for ls in range(lsmin,lslimit):
      if(type == "cdaq"):
         res = requests.post('http://es-cdaq.cms:9200/runindex_cdaq_read/minimerge/_search?pretty&size=1000','{"query":{ "bool":{"must":{"prefix":{"_id":"run'+str(run)+'"}},"must":{"term":{"ls":'+str(ls)+'}},"must":{"term":{"stream":"'+stream+'"}} }}}')
      else:
         res = requests.post('http://es-cdaq.cms:9200/runindex_minidaq_read/minimerge/_search?pretty&size=1000','{"query":{ "bool":{"must":{"prefix":{"_id":"run'+str(run)+'"}},"must":{"term":{"ls":'+str(ls)+'}},"must":{"term":{"stream":"'+stream+'"}} }}}')
      js = json.loads(res.content)

      hitlist = js['hits']['hits']#['_source']['in']
      print "statistics for run",run,"stream",stream,"ls",ls,"  number of BUs reporting mini-merge:",len(hitlist)
      tot = hitlist[0]['_source']['eolField2']

      totproc=0
      totacc=0

      for hit in hitlist:
         totproc+=hit['_source']['processed']
         totacc+=hit['_source']['accepted']
         totEoL=hit['_source']['eolField2']
         if totEoL!=tot: print 'mismatch',totEoL,tot

      if tot!=totproc:
         print "  processed",totproc,"accepted",totacc,"   EoLS total:",tot,"INCOMPLETE!!!"
      else:
         print "  processed",totproc,"accepted",totacc,"   EoLS total:",tot

      eventsInput  = eventsInput  + totproc
      eventsOutput = eventsOutput + totacc

   theMergedJSONfile = open(outMergedJSONFullPath, 'w')
   theMergedJSONfile.write(json.dumps({'data': (eventsInput, eventsOutput, 0, os.path.basename(outMergedFileFullPath), totalSize, checkSumFile, 0, tot, 0, "Tier0")}))
   theMergedJSONfile.close()
