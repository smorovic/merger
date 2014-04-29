## Source this script to launch the merging test

LIST_PRODUCERS=listProducers.txt
#LIST_MERGERS=listMergers.txt

MERGERID=lxplus0095
CATID=lxplus0095

TEST_BASE=/afs/cern.ch/user/c/ceballos/merger/hwtest
LUMI_LENGTH_MEAN=10
LUMI_LENGTH_SIGMA=0.01

INPUT_LOCATION=/afs/cern.ch/work/c/ceballos/testHW
OUTPUT_LOCATION=/afs/cern.ch/work/c/ceballos/testHW
ROOT_LOCATION=/afs/cern.ch/user/c/ceballos/merger

## defines node_name, count_args
source $TEST_BASE/tools.sh

#-------------------------------------------------------------------------------
function launch_main {
    delete_previous_runs
    kill_previous_mergers

    launch_producers mergeConfigTest 1
    launch_merger 300 optionA lxplus0095.cern.ch 1
    #launch_simple_cat 300 0 lxplus0095
    
#    launch_producers mergeConfigTest
#    launch_producers run100.cfg
#    launch_producers run200.cfg
#    launch_producers run300.cfg
#    launch_producers run400.cfg
#    launch_producers run500.cfg

} # launch_main

#-------------------------------------------------------------------------------
function kill_previous_mergers {

    for NODE in `cat $LIST_PRODUCERS | cut -d' ' -f1`; do
        COMMAND=$(cat <<'EOF'
            PS_LINE=$(ps awwx | grep python | egrep -v "grep|bash");\
            PID=$(echo $PS_LINE | awk '{print $1}');\
            kill -9 $PID
EOF
        )
        echo $NODE
        echo "      $COMMAND"
        ssh  $NODE "$COMMAND"
    done
} # kill_previous_mergers

#-------------------------------------------------------------------------------
# Expects the run number as the first argument, 
# the merging option as the second one
# the node name as the third one, if none then a list of nodes will be used
function launch_merger {
    export RUN=${1:-300}
    export THEOPTION=${2:-OptionA}
    export NODE=${3,-$MERGERID}
    export useList=${4,-0}
    if [ ${useList} == "0" ]; then

       mkdir -p $ROOT_LOCATION/${NODE};
       cp $ROOT_LOCATION/dataFlowMergerTemplate.conf \
          $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|AAA|$INPUT_LOCATION/${NODE}/unmergedDATA/run${RUN}|"  $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|BBB|$INPUT_LOCATION/${NODE}/unmergedMON|"             $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|OPTION|$THEOPTION|"                                   $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|CCC|$OUTPUT_LOCATION/mergerMini|"                     $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|DDD|$OUTPUT_LOCATION/mergerMacro|"                    $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|LOG|$ROOT_LOCATION/logFormat.conf|"                   $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       
       cp $ROOT_LOCATION/Logging.py               $ROOT_LOCATION/${NODE}/Logging_${NODE}.py;
       cp $ROOT_LOCATION/dataFlowMergerInLine     $ROOT_LOCATION/${NODE}/dataFlowMergerInLine_${NODE};
       cp $ROOT_LOCATION/Logging.py               $ROOT_LOCATION/${NODE}/Logging_${NODE}.py;
       cp $ROOT_LOCATION/cmsDataFlowMerger.py     $ROOT_LOCATION/${NODE}/cmsDataFlowMerger.py;
       cp $ROOT_LOCATION/cmsActualMergingFiles.py $ROOT_LOCATION/${NODE}/cmsActualMergingFiles.py;

       sed -i "s|dataFlowMerger.conf|$ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf|" $ROOT_LOCATION/${NODE}/dataFlowMergerInLine_${NODE};
       sed -i "s|dataFlowMerger.conf|$ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf|" $ROOT_LOCATION/${NODE}/Logging_${NODE}.py;
       sed -i "s|Logging|Logging_${NODE}|" $ROOT_LOCATION/${NODE}/dataFlowMergerInLine_${NODE};
       sed -i "s|Logging|Logging_${NODE}|" $ROOT_LOCATION/${NODE}/cmsDataFlowMerger.py;
       sed -i "s|Logging|Logging_${NODE}|" $ROOT_LOCATION/${NODE}/cmsActualMergingFiles.py;

       #rsync -aW $TEST_BASE/ $NODE:$ROOT_LOCATION/hwtest/
       COMMAND="$(cat << EOF
       (   cd $OUTPUT_LOCATION ; \
           nohup $ROOT_LOCATION/${NODE}/dataFlowMergerInLine_${NODE} \ 
       )   >& $ROOT_LOCATION/hwtest/merger_${THEOPTION}_run${RUN}_${NODE}.log &
EOF
       )"
       echo $NODE
       echo "    $COMMAND"
       ssh $NODE $COMMAND

    else
       for NODE in `cat $LIST_PRODUCERS | cut -d' ' -f1`; do

       mkdir -p $ROOT_LOCATION/${NODE};
       cp $ROOT_LOCATION/dataFlowMergerTemplate.conf \
          $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|AAA|$INPUT_LOCATION/${NODE}/unmergedDATA/run${RUN}|"  $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|BBB|$INPUT_LOCATION/${NODE}/unmergedMON|"             $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|OPTION|$THEOPTION|"                                   $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|CCC|$OUTPUT_LOCATION/mergerMini|"                     $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|DDD|$OUTPUT_LOCATION/mergerMacro|"                    $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       sed -i "s|LOG|$ROOT_LOCATION/logFormat.conf|"                   $ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf;
       
       cp $ROOT_LOCATION/Logging.py               $ROOT_LOCATION/${NODE}/Logging_${NODE}.py;
       cp $ROOT_LOCATION/dataFlowMergerInLine     $ROOT_LOCATION/${NODE}/dataFlowMergerInLine_${NODE};
       cp $ROOT_LOCATION/Logging.py               $ROOT_LOCATION/${NODE}/Logging_${NODE}.py;
       cp $ROOT_LOCATION/cmsDataFlowMerger.py     $ROOT_LOCATION/${NODE}/cmsDataFlowMerger.py;
       cp $ROOT_LOCATION/cmsActualMergingFiles.py $ROOT_LOCATION/${NODE}/cmsActualMergingFiles.py;

       sed -i "s|dataFlowMerger.conf|$ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf|" $ROOT_LOCATION/${NODE}/dataFlowMergerInLine_${NODE};
       sed -i "s|dataFlowMerger.conf|$ROOT_LOCATION/${NODE}/dataFlowMerger_${NODE}.conf|" $ROOT_LOCATION/${NODE}/Logging_${NODE}.py;
       sed -i "s|Logging|Logging_${NODE}|" $ROOT_LOCATION/${NODE}/dataFlowMergerInLine_${NODE};
       sed -i "s|Logging|Logging_${NODE}|" $ROOT_LOCATION/${NODE}/cmsDataFlowMerger.py;
       sed -i "s|Logging|Logging_${NODE}|" $ROOT_LOCATION/${NODE}/cmsActualMergingFiles.py;

       #rsync -aW $TEST_BASE/ $NODE:$ROOT_LOCATION/hwtest/
       COMMAND="$(cat << EOF
       (   cd $OUTPUT_LOCATION ; \
           nohup $ROOT_LOCATION/${NODE}/dataFlowMergerInLine_${NODE} \ 
       )   >& $ROOT_LOCATION/hwtest/merger_${THEOPTION}_run${RUN}_${NODE}.log &
EOF
       )"
       echo $NODE
       echo "    $COMMAND"
       ssh $NODE $COMMAND

       done
    fi
} # launch_merger

#-------------------------------------------------------------------------------
# Expects the config file name as the first argument
# the second argument is to allow for a subfolder depending on the node
function launch_producers {
    CONFIG=${1:-mergeConfigForReal}
    DOSUBFOLDER=${2:-0}
    TOTALBUS=$(count_args $LIST_PRODUCERS)
    for NODE in `cat $LIST_PRODUCERS | cut -d' ' -f1`; do
        #rsync -aW $TEST_BASE/ $NODE:$ROOT_LOCATION/hwtest/
	SUBFOLDER=""
	if [ ${DOSUBFOLDER} == "1" ]; then
	  SUBFOLDER=${NODE}"/"
	fi
        COMMAND="$(cat << EOF
        nohup $ROOT_LOCATION/hwtest/manageStreams.py \
            --config $ROOT_LOCATION/hwtest/$CONFIG \
            --bu $NODE \
            -i $ROOT_LOCATION/hwtest/frozen_storage/ \
            -p $INPUT_LOCATION/${SUBFOLDER} \
	    -a $TOTALBUS \
            --lumi-length-mean="$LUMI_LENGTH_MEAN" \
            --lumi-length-sigma="$LUMI_LENGTH_SIGMA" \
            >& $ROOT_LOCATION/hwtest/producer_${CONFIG}_${NODE}.log &
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        ssh $NODE $COMMAND
    done
    echo
} # launch_producers

#-------------------------------------------------------------------------------
function launch_simple_cat {
    ## The number of process per node is passed as the first arg, default=1
    RUN=${1:-300}
    LS=${2:-1}
    NODE=${3:-$CATID}
    STREAM=A
    echo $RUN
    SOURCE_BASE=$INPUT_LOCATION/unmergedDATA/run${RUN}
    DESTINATION_BASE=$OUTPUT_LOCATION/mergerMini/run${RUN}
    COMMAND="$(cat << EOF
    SOURCES="$SOURCE_BASE/run${RUN}_ls${LS}_Stream${STREAM}_*.dat";
    DESTINATION="$DESTINATION_BASE/run${RUN}_ls${LS}_Stream${STREAM}_SM.dat";
    LOG="$OUTPUT_LOCATION/cat_${LS}.log";
    (time cat \$SOURCES > \$DESTINATION) >& \$LOG &
EOF
    )"
    echo $NODE
    echo "     $COMMAND"
    ssh $NODE "$COMMAND"
    echo
} # launch_simple_cat

#-------------------------------------------------------------------------------
function delete_previous_runs {
    echo "Deleting $OUTPUT_LOCATION/{*merge*,*unmerge*} ..."
    echo "Deleting $INPUT_LOCATION/{*merge*,*unmerge*} ..."
    rm -rf         $OUTPUT_LOCATION/{*merge*,*unmerge*}
    rm -rf         $INPUT_LOCATION/{*merge*,*unmerge*}
    echo "Deleting $OUTPUT_LOCATION/*/{*merge*,*unmerge*} ..."
    echo "Deleting $INPUT_LOCATION/*/{*merge*,*unmerge*} ..."
    rm -rf         $OUTPUT_LOCATION/*/{*merge*,*unmerge*}
    rm -rf         $INPUT_LOCATION/*/{*merge*,*unmerge*}
    echo "    ... done."
} # delete_previous_runs

launch_main
