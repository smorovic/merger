#!/bin/bash

## Source this script to launch the merging test

LIST_PRODUCERS=listProducers.txt
#LIST_MERGERS=listMergers.txt
LIST_MERGERS=$LIST_PRODUCERS

LUMI_LENGTH_MEAN=2
LUMI_LENGTH_SIGMA=0.01

## Top-leve directory for the test management and control
TEST_BASE=/root/veverka/merger
## Folder for the producer inputs
FROZEN_LOCATION=/root/testHW/frozen
## Top-level directory for the producer outputs / merger inputs
INPUT_LOCATION=/lustre/testHW
## Top-level directory for the merger outputs
OUTPUT_LOCATION=/lustre/testHW
## Top level directory for the producer and merger scripts used during the test
ROOT_LOCATION=/root/testHW

## defines node_name, count_args
source $TEST_BASE/hwtest/tools.sh

#-------------------------------------------------------------------------------
function launch_main {
    echo "+ Launching the test ..."
    kill_previous_mergers_and_producers
    delete_previous_runs
    launch_mergers 100 optionB
    launch_producers run100.cfg 1
    echo "+ ... done. Finished launching the test."
} # launch_main


#-------------------------------------------------------------------------------
function kill_previous_mergers_and_producers {
    echo "++ Killing previous mergers and producers ..."
    NODES=$(parse_machine_list $LIST_MERGERS)
    NODES="$NODES $(parse_machine_list $LIST_PRODUCERS)"
    for NODE in $NODES; do
        COMMAND="$(cat <<'EOF'
            PROCESS_IDS=$(ps awwx |\
                grep python |\
                egrep -v "grep|bash" |\
                awk '{print $1}');\
            if [[ ! -z "$PROCESS_IDS" ]]; then                              \
                kill -9 $PROCESS_IDS                                       ;\
            fi
EOF
            )"
        echo_and_ssh $NODE "$COMMAND"
    done
    printf "++ ... done. Finished killing previous mergers and producers.\n\n"
} # kill_previous_mergers_and_producers


#-------------------------------------------------------------------------------
# Expects the run number as the first argument,
# the merging option as the second one
function launch_mergers {
    echo "++ Launching mergers ..."
    RUN=${1:-300}
    OPTION=${2:-OptionA}
    for NODE in $(parse_machine_list $LIST_MERGERS); do
        launch_merger $RUN $OPTION $NODE
    done
    printf "++ ... done. Finished launching mergers.\n\n"
} # launch_mergers


#-------------------------------------------------------------------------------
# Expects the run number as the first argument,
# the merging option as the second one
# the node name as the third one
function launch_merger {
    RUN=${1:-300}
    THEOPTION=${2:-OptionA}
    NODE=${3:-}
    if [ -z $NODE ]; then
        echo "launch_merger: ERROR: no target host name specified\!"
        return 1
    fi

    ## Create a custom config file
    CONFIG=$TEST_BASE/dataFlowMerger.conf
    /bin/cp $TEST_BASE/dataFlowMergerTemplate.conf $CONFIG
    sed -e "s|AAA|$INPUT_LOCATION/${NODE}/unmergedDATA/run${RUN}|" \
        -e "s|BBB|$INPUT_LOCATION/${NODE}/unmergedMON|"            \
        -e "s|OPTION|$THEOPTION|"                                  \
        -e "s|CCC|$OUTPUT_LOCATION/mergerMini|"                    \
        -e "s|DDD|$OUTPUT_LOCATION/mergerMacro|"                   \
        -e "s|LOG|$ROOT_LOCATION/logFormat.conf|"                  \
        -i $CONFIG

    ## Make sure that the remote folder to contain source code exists
    echo_and_ssh $NODE "mkdir -p $ROOT_LOCATION/$NODE"

    ## Sync the common source code to the remote node
    rsync -aW $TEST_BASE/ $NODE:$ROOT_LOCATION/$NODE

    ## Customize source code: update hard-cody path to the config file
    mkdir -p $TEST_BASE/hwtest/${NODE}
    /bin/cp $TEST_BASE/{dataFlowMergerInLine,Logging.py} \
        $TEST_BASE/hwtest/${NODE}
    
    sed -i "s|dataFlowMerger.conf|$ROOT_LOCATION/$NODE/dataFlowMerger.conf|" \
        $TEST_BASE/hwtest/${NODE}/{dataFlowMergerInLine,Logging.py}

    ## Sync the custom source code to the remote node
    rsync -aW $TEST_BASE/hwtest/${NODE}/ $NODE:$ROOT_LOCATION/$NODE

    ## Clean up custom sources
    rm -rf $TEST_BASE/hwtest/${NODE}

    ## Create the launch command
    COMMAND="$(cat << EOF
    (   cd $OUTPUT_LOCATION ; \
        nohup $ROOT_LOCATION/${NODE}/dataFlowMergerInLine \
    )   >& $ROOT_LOCATION/hwtest/merger_${THEOPTION}_run${RUN}_${NODE}.log &
EOF
    )"
    
    ## Launch the merger
    echo_and_ssh $NODE "$COMMAND"
} # launch_merger


#-------------------------------------------------------------------------------
# Expects the config file name as the first argument
# the second argument is to allow for a subdirectory depending on the node
function launch_producers {
    echo "++ Launching producers ..."
    CONFIG=${1:-mergeConfigForReal}
    DOSUBFOLDER=${2:-0}
    TOTALBUS=$(count_args $(parse_machine_list $LIST_PRODUCERS))
    for NODE in $(parse_machine_list $LIST_PRODUCERS); do
        rsync -aW $TEST_BASE/ $NODE:$ROOT_LOCATION/
        SUBFOLDER=""
        if [ ${DOSUBFOLDER} == "1" ]; then
            SUBFOLDER=${NODE}"/"
        fi
        COMMAND="$(cat << EOF
        nohup $ROOT_LOCATION/hwtest/manageStreams.py \
            --config $ROOT_LOCATION/hwtest/$CONFIG \
            --bu $NODE \
            -i $FROZEN_LOCATION \
            -p $INPUT_LOCATION/${SUBFOLDER} \
            -a $TOTALBUS \
            --lumi-length-mean=$LUMI_LENGTH_MEAN \
            --lumi-length-sigma=$LUMI_LENGTH_SIGMA \
            >& $ROOT_LOCATION/hwtest/producer_${CONFIG}_${NODE}.log &
EOF
        )"
        echo_and_ssh $NODE "$COMMAND"
    done
    printf "++ ... done. Finished launching producers.\n\n"
} # launch_producers


#-------------------------------------------------------------------------------
function launch_simple_cat {
    ## The number of process per node is passed as the first arg, default=1
    RUN=${1:-300}
    LS=${2:-1}
    NODE=$3
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
    echo_and_ssh $NODE $COMMAND
    echo
} # launch_simple_cat


#-------------------------------------------------------------------------------
function delete_previous_runs {
    echo "++ Deleting previous runs ..."
    echo_and_rm "$INPUT_LOCATION/*merge*/run*"
    echo_and_rm "$OUTPUT_LOCATION/*merge*/run*"
    echo_and_rm "$INPUT_LOCATION/*/*merge*/run*"
    echo_and_rm "$OUTPUT_LOCATION/*/*merge*/run*"
    printf "++ ... done. Finished deleting previous runs.\n\n"
} # delete_previous_runs


#-------------------------------------------------------------------------------
function echo_and_ssh {
    NODE=$1
    COMMAND="$2"
    echo "+++ $NODE"
    ## Format the command for printing, add more line breaks.
    FORMATTED_COMMAND="$(echo $COMMAND |\
                         tr ';' '\n' |\
                         sed -e 's/ -/ \\\n    -/g' -e 's/ >/ \\\n    >/g' |\
                         sed -E 's/^/    /g')"
    echo "$FORMATTED_COMMAND"
    ssh $NODE "$COMMAND"
}  ## echo_and_ssh


#-------------------------------------------------------------------------------
function parse_machine_list {
    ## sed removes Bash/Python-style comments starting with `#'
    ## awk makes sure to ignore white space around the node name
    echo "$(sed 's/#.*$//' $1 | awk '{print $1}')"
} ## parse_machine_list


#-------------------------------------------------------------------------------
function echo_and_rm {
    if [[ ! -z "$@" ]]; then
        printf "+++ Deleting $@ ..."
        rm -rf $@
        printf " done.\n"
    fi
} ## echo_and_rm

#-------------------------------------------------------------------------------
function quote {
    echo "\`"${@}"'"
} ## quote

launch_main
