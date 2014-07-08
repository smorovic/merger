# -*- coding: utf-8 -*-
import os
import sys
import logging
import logging.config
import inspect
from configobj import ConfigObj

_default_config_file = "dataFlowMerger.conf"

#_______________________________________________________________________________
def getLogger(config_filename = _default_config_file):
    loggingConfigFile = get_logging_config_file(config_filename)
    logging.config.fileConfig(loggingConfigFile)
    caller_module = get_caller_module_basename()
    logger = logging.getLogger('Merger.' + caller_module)
    return logger

#_______________________________________________________________________________
def get_logging_config_file(filename):
    check_file(filename)
    config = ConfigObj(filename)
    return config['Misc']['logConfigFile']
### get_logging_config_file

#_______________________________________________________________________________
def check_file(filename):
    if not os.path.isfile(filename):
        message = "{0}: Configuration file not found: {1}!".format(__name__,
                                                                   filename)
        raise RunTimeError(message)
## check_file

#_______________________________________________________________________________
def get_caller_module_basename():
    frame = inspect.stack()[1]
    module_name = inspect.getfile(frame[0])
    start = module_name.rfind('/')+1
    dot_position = module_name.rfind('.')
    end = dot_position if dot_position != -1 else len(module_name)
    return module_name[start:end]
## get_caller_module_basename

log = getLogger()
