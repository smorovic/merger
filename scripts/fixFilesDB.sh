#/bin/sh

export RUN=$1;
ls -l /store/lustre/mergeMacro/run${RUN} > temp_fix0;
if [ $? -eq 0 ]
then
  rm -f temp_fix0;
else
  rm -f temp_fix0;
  echo "run "${RUN}" does no exist";
  exit 1;
fi

source ~ceballos/merger/scripts/setCMSSW.sh;

cd /opt/python/smhook;
./check-run.sh --runnumber=${RUN} --state=FILES_TRANS_NEW|grep run${RUN}|awk '{print$1}' > temp_fix0;
awk '{split($1,a,".");printf("%s\n",a[1]);}' temp_fix0 > temp_fix1;

for fileName in `cat temp_fix1`; do

~ceballos/merger/scripts/checkFiles.py --p $fileName;
if [ $? -eq 0 ]
then
  echo "Correct file "${fileName}" ---> updating database";
  ./check-run.sh --runnumber=${RUN} --checksum --nochecksum --debug --force --fix --nodbupdate --pattern=${fileName}.dat;
else
  echo "Corrupted file "${fileName};
fi

done

rm -f temp_fix0 temp_fix1;
