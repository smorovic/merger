#!/usr/bin/env python
import os, time, sys, getopt
import shutil
import json

# program to merge (cat) files

# expected format for all files
#dataType.run.lumisec.stream.fileN.fileType.dat
#	0   1       2      3     4	  5
#
# std: data files, mon: monitor files, met: meta data files, eol: end-of-lumi files

# 3 folders are needed:
# unmerged: where input files arrive
# merged: where input files are merged
# done: where merge files go once the LS is complete
#       split in run number to avoid many files in a single folder

"""
Check if file is completed
"""
def is_completed(filepath):
   """Checks if a file is completed by opening it in append mode.
   If no exception thrown, then the file is not locked.
   """
   completed = False
   file_object = None
   buffer_size = 8
   # Opening file in append mode and read the first 8 characters
   file_object = open(filepath, 'a', buffer_size)
   if file_object:
      completed = True
      file_object.close()

   return completed

"""
Do actual merging
"""
def doMerging():
   before = dict ([(f, None) for f in os.listdir (path_to_watch)])
   while 1:
     if(debug > 0): time.sleep (1)
     if(debug > 0): print "Begin iteration"
     after = dict ([(f, None) for f in os.listdir (path_to_watch)])     
     afterString = [f for f in after]
     added = [f for f in after if not f in before]
     removed = [f for f in before if not f in after]
     if added: 
        if(debug > 0): print "Added: ", ", ".join (added)
     if removed: 
        if(debug > 0): print "Removed: ", ", ".join (removed)
     for i in range(0, len(afterString)):
	fileNameString = afterString[i].split('.')
	if(debug > 0): print "FILE:", fileNameString
	if(fileNameString[5] == "std"):
	   # assumed that the file structure is identical, just replacing the file type
	   inputFile0 = os.path.join(path_to_watch, afterString[i])
	   inputFile1 = inputFile0.replace(".std.dat",".mon.dat")
	   inputFile2 = inputFile0.replace(".std.dat",".met.json")
	   if(debug > 0): print "files: %s - %s - %s" % (inputFile0,inputFile1,inputFile2)
	   """
	   opening a file is CPU expensive, then we open the last one (meta data) only
	   """
           if os.path.exists(inputFile0) and os.path.exists(inputFile1) and os.path.exists(inputFile2) and is_completed(inputFile2):
	      outFile0 = "%s/%s.%s.%s.%s.%s.dat" % (path_to_merge,fileNameString[0],fileNameString[1],fileNameString[2],fileNameString[3],"std")
	      outFile1 = "%s/%s.%s.%s.%s.%s.dat" % (path_to_merge,fileNameString[0],fileNameString[1],fileNameString[2],fileNameString[3],"mon")

	      initMergingTime = time.time() 
	      # First way to do the merging
	      msg  = "cat %s >> %s;" % (inputFile0,outFile0)
	      msg += "cat %s >> %s;" % (inputFile1,outFile1)
	      if(debug > 0): print msg
	      os.system(msg)

	      # Second way to do the merging
              #open(outFile0,'wb').write(open(inputFile0).read())
              #open(outFile1,'wb').write(open(inputFile1).read())

	      # Third way to do the merging
              #file_object_output0 = open(outFile0,'a')
              #file_object_input0  = open(inputFile0)
	      #shutil.copyfileobj(file_object_input0, file_object_output0)
              #file_object_output1 = open(outFile1,'a')
              #file_object_input1  = open(inputFile1)
	      #shutil.copyfileobj(file_object_input1, file_object_output1)

              # Remove the already merged files
	      os.remove(inputFile0)
	      os.remove(inputFile1)
	      endMergingTime = time.time() 
	      if(debug >= 0): print "Time for merging(%s): %f" % (fileNameString[3],endMergingTime-initMergingTime)

              # read metadata information
              file_met = open(inputFile2, 'r').read()
              line_json = json.loads(file_met)
              if(debug > 0): print line_json
	      lineStripString = [line_json['inputEvents'], line_json['outputEvents']]

              # read global metadata information for the given LS
	      outFile2 = "%s/%s.%s.%s.%s.%s.json" % (path_to_merge,fileNameString[0],fileNameString[1],fileNameString[2],fileNameString[3],"met")
              if os.path.exists(outFile2):
        	 file_metOutput = open(outFile2, 'r').read()
        	 line_json = json.loads(file_metOutput)
		 lineStripStringOutput = [line_json['inputEvents'], line_json['outputEvents']]
	      else:
		 lineStripStringOutput = [0, 0]

              # sum the events are being merged
	      lineStripStringOutput[0] = float(lineStripString[0]) + float(lineStripStringOutput[0])
	      lineStripStringOutput[1] = float(lineStripString[1]) + float(lineStripStringOutput[1])

              # re-creating the file for the given LS
              if os.path.exists(outFile2):
	         os.remove(outFile2)
	      file_metOutput = open(outFile2, 'w')
              file_metOutput.write(json.dumps({'inputEvents': int(lineStripStringOutput[0]), 'outputEvents': int(lineStripStringOutput[1])}, sort_keys=True, indent=4, separators=(',', ': ')))
              file_metOutput.close()
	      os.remove(inputFile2)
	elif(fileNameString[5] == "eol"):
           file_eol = open(os.path.join(path_to_watch, afterString[i]), 'r').read()
           line_json = json.loads(file_eol)
	   lineStripString = [line_json['inputEvents'], line_json['outputEvents']]

	   outFile0 = "%s/%s.%s.%s.%s.%s.json" % (path_to_merge,fileNameString[0],fileNameString[1],fileNameString[2],fileNameString[3],"met")
           if os.path.exists(outFile0):
              file_metOutput = open(outFile0, 'r').read()
              line_json = json.loads(file_metOutput)
	      lineStripStringOutput = [line_json['inputEvents'], line_json['outputEvents']]
	   else:
	      lineStripStringOutput = [0, 0]
	  
	   # if both numbers are identical, then done with that LS
	   if(lineStripString[1] == lineStripStringOutput[1]):
	      # moving std and mon files too
	      outFile1 = "%s/%s.%s.%s.%s.%s.dat" % (path_to_merge,fileNameString[0],fileNameString[1],fileNameString[2],fileNameString[3],"std")
	      outFile2 = "%s/%s.%s.%s.%s.%s.dat" % (path_to_merge,fileNameString[0],fileNameString[1],fileNameString[2],fileNameString[3],"mon")
              if not os.path.exists(os.path.join(path_to_done,fileNameString[1])):
                 os.mkdir(os.path.join(path_to_done,fileNameString[1]))
	      msg =  "mv %s %s/%s/;" % (outFile0,path_to_done,fileNameString[1])
	      msg += "mv %s %s/%s/;" % (outFile1,path_to_done,fileNameString[1])
	      msg += "mv %s %s/%s/;" % (outFile2,path_to_done,fileNameString[1])
	      if(debug > 0): print msg
	      if(debug > 0): print "eol(%s) exist and complete" % outFile0
	      os.system(msg)
	      os.remove(os.path.join(path_to_watch, afterString[i]))
	   else:
	      if(debug > 0): print "eol(%s) exist, but not complete yet" % outFile0
	  
     before = after

"""
Main
"""
valid = ['path_to_watch=', 'path_to_merge=',  'path_to_done=',
         'debug=', 'help']

usage =  "Usage: listdir.py --path_to_watch=<path_to_watch>\n"
usage += "                  --path_to_merge=<path_to_merge>\n"
usage += "                  --path_to_done=<path_to_done>\n"
usage += "                  --debug=<0>\n"

try:
   opts, args = getopt.getopt(sys.argv[1:], "", valid)
except getopt.GetoptError, ex:
   print usage
   print str(ex)
   sys.exit(1)

path_to_watch = "unmerged"
path_to_merge = "merged"
path_to_done  = "done"
debug         = 0

for opt, arg in opts:
   if opt == "--help":
      print usage
      sys.exit(1)
   if opt == "--path_to_watch":
      path_to_watch = arg
   if opt == "--path_to_merge":
      path_to_merge = arg
   if opt == "--path_to_done":
      path_to_done = arg
   if opt == "--debug":
      debug = arg

if not os.path.exists(path_to_watch):
   msg = "path_to_watch Not Found: %s" % path_to_watch
   raise RuntimeError, msg

if not os.path.exists(path_to_merge):
   os.mkdir(path_to_merge)

if not os.path.exists(path_to_done):
   os.mkdir(path_to_done)

doMerging()
