#!/bin/sh

# test on fu-c2f13-39-01 or mrg-c2f13-35-01

studyFolder=$1;

rm -rf testArea;
mkdir testArea;
cd testArea;
myDir=$PWD;

cd $myDir;

ls $studyFolder | grep dat > list_files.txt;

for file in `cat list_files.txt | cut -d' ' -f1`; do
  echo "****************************FILE: "$file;
  DiagStreamerFile $studyFolder/$file
done
