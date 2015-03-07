"""
Do json dat files for 0 event cases using EoLS files
"""
def doMakeJsonStreams(inputDataFolder,afterString,iniIDict,outputMergedFolder,outputEndName,doRemoveFiles,debug):
   try:
      fileNameString = afterString.split('_')
      inputJsonFile = os.path.join(inputDataFolder, afterString)

      # avoid empty files
      if(os.path.exists(inputJsonFile) and os.path.getsize(inputJsonFile) == 0): return

      # moving the file to avoid issues
      inputJsonRenameFile = inputJsonFile.replace(".jsn","_TEMP.jsn")
      shutil.move(inputJsonFile,inputJsonRenameFile)

      settingsEoLS = readJsonFile(inputJsonRenameFile,debug)

      if("bad" in settingsEoLS): return

      eventsEoLS    = int(settingsEoLS['data'][0])
      filesEoLS     = int(settingsEoLS['data'][1])
      eventsAllEoLS = int(settingsEoLS['data'][2])
      NLostEvents   = int(settingsEoLS['data'][3])
      if(eventsEoLS == 0):

         for streamName in iniIDict:                
	    outMergedJSON = fileNameString[0] + "_" + fileNameString[1] + "_" + streamName + "_" + outputEndName + ".jsn";
	    outMergedJSONFullPath = os.path.join(outputMergedFolder, outMergedJSON)
	    outMergedJSONFullPathStable = outputMergedFolder + "/../" + outMergedJSON

	    # input events in that file, all input events, file name, output events in that files, number of merged files
	    # only the first three are important
	    theMergedJSONfile = open(outMergedJSONFullPath, 'w')
	    theMergedJSONfile.write(json.dumps({'data': (eventsEoLS, 0, 0, "", 0, 0, filesEoLS, eventsAllEoLS, NLostEvents)}))
	    theMergedJSONfile.close()

	    shutil.move(outMergedJSONFullPath,outMergedJSONFullPathStable)

      if(doRemoveFiles == "True"):
	 os.remove(inputJsonRenameFile)

#///////////////////////////////////////////

   except Exception,e:
      log.error("doMakeJsonStreams failed {0} - {1}".format(inputJsonFile,e))

	  # loop over JSON files, looking for EoLS files
	  for i in range(0, len(afterString)):
	     if not afterString[i].endswith("EoLS.jsn"): continue
	     if "index" in afterString[i]: continue
	     if afterString[i].endswith("recv"): continue
	     if "BoLS" in afterString[i]: continue
	     if "EoR" in afterString[i]: continue
	     if "TEMP" in afterString[i]: continue

	     if(len(iniIDict.keys()) == 0):
		theStoreIniArea = outputSMMergedFolder + "/../"
		# reading the list of files in the given folder
		after = dict([(f, None) for f in os.listdir(theStoreIniArea)])
		afterStringIniNoSorted = [f for f in after]
		afterStringIni = sorted(afterStringIniNoSorted, reverse=False)
		for nb in range(0, len(afterStringIni)):
        	   if afterStringIni[nb].endswith(".ini"):
        	      fileIniString = afterStringIni[nb].split('_')
   		      key = (fileIniString[2])

   		      if "DQM" in key:
   	        	  continue
   		      if "streamError" in key:
   	        	  continue
   		      if "HLTRates" in key:
   	        	  continue
   		      if "L1Rates" in key:
   	        	  continue

   		      if key in iniIDict.keys():
   			 iniIDict[key].append(fileIniString[3].split('.ini')[0])
   		      else:
        		 iniIDict.update({key: [fileIniString[3].split('.ini')[0]]})

             process = thePool.apply_async(doMakeJsonStreams, [inputDataFolder,afterString[i],iniIDict,outputMergedFolder,outputEndName,doRemoveFiles,debug])
