#!/usr/bin/env python
import os

from Logging import getLogger
log = getLogger()

"""
making theOutput folders
"""
def doMakeFolders(theOutputMergedFolderJSNS, theOutputSMMergedFolderJSNS, theOutputDQMMergedFolderJSNS, 
                  theOutputMergedFolderDATA, theOutputSMMergedFolderDATA, theOutputDQMMergedFolderDATA, 
                  theOutputBadFolder, theOutputSMBadFolder, theOutputSMRecoveryFolder):

   if not os.path.exists(theOutputMergedFolderJSNS):
      try:
          os.makedirs(theOutputMergedFolderJSNS)
          msg = "sudo lfs setstripe -c 1 -S 1m {0}".format(theOutputMergedFolderJSNS)
          os.system(msg)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputMergedFolderJSNS))

   if not os.path.exists(theOutputSMMergedFolderJSNS):
      try:
          os.makedirs(theOutputSMMergedFolderJSNS)
          msg = "sudo lfs setstripe -c 1 -S 1m {0}".format(theOutputSMMergedFolderJSNS)
          os.system(msg)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputSMMergedFolderJSNS))

   if not os.path.exists(theOutputDQMMergedFolderJSNS):
      try:
          os.makedirs(theOutputDQMMergedFolderJSNS)
          msg = "sudo lfs setstripe -c 1 -S 1m {0}".format(theOutputDQMMergedFolderJSNS)
          os.system(msg)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputDQMMergedFolderJSNS))

   if not os.path.exists(theOutputMergedFolderDATA):
      try:
          os.makedirs(theOutputMergedFolderDATA)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputMergedFolderDATA))

   if not os.path.exists(theOutputSMMergedFolderDATA):
      try:
          os.makedirs(theOutputSMMergedFolderDATA)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputSMMergedFolderDATA))

   if not os.path.exists(theOutputDQMMergedFolderDATA):
      try:
          os.makedirs(theOutputDQMMergedFolderDATA)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputDQMMergedFolderDATA))

   if not os.path.exists(theOutputBadFolder):
      try:
          os.makedirs(theOutputBadFolder)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputBadFolder))

   if not os.path.exists(theOutputSMBadFolder):
      try:
          os.makedirs(theOutputSMBadFolder)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputSMBadFolder))

   if not os.path.exists(theOutputSMRecoveryFolder):
      try:
          os.makedirs(theOutputSMRecoveryFolder)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputSMRecoveryFolder))
