#!/bin/sh

myDir=$PWD/;

cd /opt/offline/;
source $PWD/cmsset_default.sh;
cd slc6_amd64_gcc491/cms/cmssw/CMSSW_7_4_14/;
eval `scramv1 runtime -sh`;

cd $myDir;
