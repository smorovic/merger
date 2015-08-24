#!/usr/bin/env python
import os

from Logging import getLogger
log = getLogger()

"""
making theOutput folders
"""
def doMakeFolders(theOutputMergedFolder, theOutputSMMergedFolder, 
      theOutputDQMMergedFolder, theOutputECALMergedFolder, theOutputBadFolder, 
      theOutputSMBadFolder, theOutputSMRecoveryFolder):

   if not os.path.exists(theOutputMergedFolder):
      try:
          os.makedirs(theOutputMergedFolder)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputMergedFolder))

   if not os.path.exists(theOutputSMMergedFolder):
      try:
          os.makedirs(theOutputSMMergedFolder)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputSMMergedFolder))

   if not os.path.exists(theOutputDQMMergedFolder):
      try:
          os.makedirs(theOutputDQMMergedFolder)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputDQMMergedFolder))

   if not os.path.exists(theOutputECALMergedFolder):
      try:
          os.makedirs(theOutputECALMergedFolder)
      except Exception, e:
          log.warning(
          "Directory {0} has just been created by someone else...".format(
          theOutputECALMergedFolder))

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
