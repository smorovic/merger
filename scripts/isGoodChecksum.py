#!/usr/bin/env python

import sys,json,os
import zlib

def main():
    for fname in sys.argv[1:]:
        try:
            checkSumFile=1
            with open(fname, 'r') as fsrc:
               length=16*1024
               while 1:
            	  buf = fsrc.read(length)
            	  if not buf:
            	     break
            	  checkSumFile=zlib.adler32(buf,checkSumFile)

            checkSumFile = checkSumFile & 0xffffffff

    	    jsonName = fname.replace(".dat",".jsn")
	    if(not os.path.exists(jsonName)):
               jsonName = jsonName.replace("data","jsns")
    	    settings_textI = open(jsonName, "r").read()
    	    settings       = json.loads(settings_textI)
    	    checkSumJson   = int(settings['data'][5])
    	    if(checkSumFile != checkSumJson):
	       print "BAD  ",checkSumFile,checkSumJson
    	    else:
	       print "GOOD ",checkSumFile,checkSumJson
        except Exception, e:
            print e

if __name__ == '__main__':
    main()
