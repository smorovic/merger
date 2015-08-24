#!/bin/sh

myDir=$PWD/;

cd /opt/offline/;
source $PWD/cmsset_default.sh;
cd slc6_amd64_gcc481/cms/cmssw/CMSSW_7_3_2/;
eval `scramv1 runtime -sh`;

cd $myDir;
