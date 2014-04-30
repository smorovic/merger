## Source this script to launch the merging test

LIST_PRODUCERS=listProducers.txt
LIST_MERGERS=listMergers.txt

MERGERID=lxplus0095
CATID=lxplus0095

TEST_BASE=/root/testHWGui/merger
LUMI_LENGTH_MEAN=20
LUMI_LENGTH_SIGMA=0.01

INPUT_LOCATION=/lustre/testHWGui
OUTPUT_LOCATION=/lustre/testHWGui
ROOT_LOCATION=/root/testHWGui/merger

## defines node_name, count_args
source $TEST_BASE/hwtest/tools.sh

#-------------------------------------------------------------------------------
function launch_main {
    kill_previous_mergers
    delete_previous_runs

    launch_producers run100.cfg 1
    launch_merger 100 optionC wbua-TME-ComputeNode7 1
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

    for NODE in `cat $LIST_MERGERS | cut -d' ' -f1`; do
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

       mkdir -p $TEST_BASE/hwtest/${NODE};
       cp $ROOT_LOCATION/dataFlowMergerTemplate.conf \
          $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|AAA|$INPUT_LOCATION/unmergedDATA/run${RUN}|"          $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|BBB|$INPUT_LOCATION/unmergedMON|"                     $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|OPTION|$THEOPTION|"                                   $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|CCC|$OUTPUT_LOCATION/mergerMini|"                     $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|DDD|$OUTPUT_LOCATION/mergerMacro|"                    $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|LOG|$ROOT_LOCATION/logFormat.conf|"                   $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       
       cp $ROOT_LOCATION/configobj.py             $TEST_BASE/hwtest/${NODE}/configobj.py;
       cp $ROOT_LOCATION/Logging.py               $TEST_BASE/hwtest/${NODE}/Logging.py;
       cp $ROOT_LOCATION/dataFlowMergerInLine     $TEST_BASE/hwtest/${NODE}/dataFlowMergerInLine;
       cp $ROOT_LOCATION/Logging.py               $TEST_BASE/hwtest/${NODE}/Logging.py;
       cp $ROOT_LOCATION/cmsDataFlowMerger.py     $TEST_BASE/hwtest/${NODE}/cmsDataFlowMerger.py;
       cp $ROOT_LOCATION/cmsActualMergingFiles.py $TEST_BASE/hwtest/${NODE}/cmsActualMergingFiles.py;

       sed -i "s|dataFlowMerger.conf|$TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf|" $TEST_BASE/hwtest/${NODE}/dataFlowMergerInLine;
       sed -i "s|dataFlowMerger.conf|$TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf|" $TEST_BASE/hwtest/${NODE}/Logging.py;

       rsync -aW $TEST_BASE/ $NODE:$ROOT_LOCATION/;
       COMMAND="$(cat << EOF
       (   cd $OUTPUT_LOCATION ; \
           nohup $TEST_BASE/hwtest/${NODE}/dataFlowMergerInLine \ 
       )   >& $ROOT_LOCATION/hwtest/merger_${THEOPTION}_run${RUN}_${NODE}.log &
EOF
       )"
       echo $NODE
       echo "     $COMMAND"
       ssh $NODE "$COMMAND"

    else
       for NODE in `cat $LIST_PRODUCERS | cut -d' ' -f1`; do

       mkdir -p $TEST_BASE/hwtest/${NODE};
       cp $ROOT_LOCATION/dataFlowMergerTemplate.conf \
          $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|AAA|$INPUT_LOCATION/${NODE}/unmergedDATA/run${RUN}|"  $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|BBB|$INPUT_LOCATION/${NODE}/unmergedMON|"             $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|OPTION|$THEOPTION|"                                   $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|CCC|$OUTPUT_LOCATION/mergerMini|"                     $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|DDD|$OUTPUT_LOCATION/mergerMacro|"                    $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       sed -i "s|LOG|$ROOT_LOCATION/logFormat.conf|"                   $TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf;
       
       cp $ROOT_LOCATION/configobj.py             $TEST_BASE/hwtest/${NODE}/configobj.py;
       cp $ROOT_LOCATION/Logging.py               $TEST_BASE/hwtest/${NODE}/Logging.py;
       cp $ROOT_LOCATION/dataFlowMergerInLine     $TEST_BASE/hwtest/${NODE}/dataFlowMergerInLine;
       cp $ROOT_LOCATION/Logging.py               $TEST_BASE/hwtest/${NODE}/Logging.py;
       cp $ROOT_LOCATION/cmsDataFlowMerger.py     $TEST_BASE/hwtest/${NODE}/cmsDataFlowMerger.py;
       cp $ROOT_LOCATION/cmsActualMergingFiles.py $TEST_BASE/hwtest/${NODE}/cmsActualMergingFiles.py;

       sed -i "s|dataFlowMerger.conf|$TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf|" $TEST_BASE/hwtest/${NODE}/dataFlowMergerInLine;
       sed -i "s|dataFlowMerger.conf|$TEST_BASE/hwtest/${NODE}/dataFlowMerger.conf|" $TEST_BASE/hwtest/${NODE}/Logging.py;

       rsync -aW $TEST_BASE/ $NODE:$ROOT_LOCATION/;
       COMMAND="$(cat << EOF
       (   cd $OUTPUT_LOCATION ; \
           nohup $TEST_BASE/hwtest/${NODE}/dataFlowMergerInLine \ 
       )   >& $ROOT_LOCATION/hwtest/merger_${THEOPTION}_run${RUN}_${NODE}.log &
EOF
       )"
       echo $NODE
       echo "     $COMMAND"
       ssh $NODE "$COMMAND"

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
        rsync -aW $TEST_BASE/ $NODE:$ROOT_LOCATION/;
	SUBFOLDER=""
	if [ ${DOSUBFOLDER} == "1" ]; then
	  SUBFOLDER=${NODE}"/"
	fi
        COMMAND="$(cat << EOF
        nohup $ROOT_LOCATION/hwtest/manageStreams.py \
            --config $ROOT_LOCATION/hwtest/$CONFIG \
            --bu $NODE \
            -i $INPUT_LOCATION/frozen_storage/ \
            -p $INPUT_LOCATION/${SUBFOLDER} \
	    -a $TOTALBUS \
            --lumi-length-mean="$LUMI_LENGTH_MEAN" \
            --lumi-length-sigma="$LUMI_LENGTH_SIGMA" \
            >& $ROOT_LOCATION/hwtest/producer_${CONFIG}_${NODE}.log &
EOF
        )"
        echo $NODE
        echo "     $COMMAND"
        ssh $NODE "$COMMAND"
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
    rsync -aW $TEST_BASE/ $NODE:$ROOT_LOCATION/;
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
