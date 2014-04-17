## Source this script to launch the merging test

## Indexes of all the nodes to be used
#NODE_INDEXES="$( echo {1..10} {12..14} 16)"
NODE_INDEXES="$(echo {1..10} {12..14} 16)"
MERGER1_INDEXES="$(echo {1..2})"
MERGER_INDEX="13"
MERGERA_INDEX="14"
# PRODUCER_INDEXES="$(echo {1..9} 12)"
PRODUCER_INDEXES="$(echo {1..2})"
SIMPLE_CAT_A_INDEXES="$(echo {1..4})"

#SIMPLE_CAT_A_INDEXES="$(echo {1..2})"
TEST_BASE=/root/merger/hwtest
LUMI_LENGTH_MEAN=10
LUMI_LENGTH_SIGMA=0.01

## defines node_name, count_args
source $TEST_BASE/tools.sh

#-------------------------------------------------------------------------------
function launch_main {
    delete_previous_runs
    kill_previous_mergers

    ## launch_merger <node> <run>
#     launch_merger_0 12 100
#     launch_merger_0 13 200
#     launch_merger_0 14 300
#     launch_merger_0 15 400
#     launch_merger_0 16 500
    
    launch_mergers_1
    
    launch_producers run100.cfg
#     launch_producers run200.cfg
#     launch_producers run300.cfg
#     launch_producers run400.cfg
#     launch_producers run500.cfg
    # launch_producers_A
    # launch_simple_cat_A 10

} # launch_main

#-------------------------------------------------------------------------------
function kill_previous_mergers {
    for i in $NODE_INDEXES; do
        NODE=$(node_name $i)
        COMMAND=$(cat <<'EOF'
            PS_LINE=$(ps awwx | grep doMerg | egrep -v "grep|bash");\
            PID=$(echo $PS_LINE | awk '{print $1}');\
            kill $PID
EOF
        )
        echo "$COMMAND"
        ssh $NODE "$COMMAND"
    done
} # kill_previous_mergers


#-------------------------------------------------------------------------------
# Expects the node index as the first argument and the run number as
# the second one. Use node=$MERGER_INDEX and run=300 as defaults.
# as default
function launch_merger_0 {
    NODE_INDEX=${1:-$MERGER_INDEX}
    RUN=${2:-300}
    NODE=$(node_name $NODE_INDEX)
    rsync -aW $TEST_BASE/ $NODE:/root/testHW/
    COMMAND="$(cat << EOF
    (   cd /lustre/testHW ; \
        nohup /root/testHW/doMergingFromList_ForTests.py \
            --expectedBUs=$(count_args $PRODUCER_INDEXES) \
            --option=0 \
            --paths_to_watch="/lustre/testHW/unmergedMON/Run${RUN}" \
    )   >& /root/testHW/merger_opt0_run${RUN}_${i}.log &
EOF
    )"
    echo $NODE
    echo "    $COMMAND"
    ssh $NODE $COMMAND
    echo
} # launch_merger_0


#-------------------------------------------------------------------------------
## Only for backwrad compatibility
function launch_mergerA_0 {
    launch_merger_0 $MERGERA_INDEX 500
} # launch_mergerA_0


#-------------------------------------------------------------------------------
function launch_mergers_1 {
    for i in $MERGER1_INDEXES; do
        NODE=$(node_name $i)
        rsync -aW $TEST_BASE/ $NODE:/root/testHW/
        COMMAND="$(cat << EOF
        (   cd /lustre/testHW && \
            nohup /root/testHW/doMergingFromList_ForTests.py \
                --expectedBUs=1 \
                --option=1 \
                --paths_to_watch="/lustre/testHW/unmergedMON/Run*" \
                --bu=$i \
        )   >& /root/testHW/merger_opt1_${i}.log &
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        ssh $NODE "$COMMAND"
    done
    echo
} # launch_mergers_1


#-------------------------------------------------------------------------------
function launch_mergers_2 {
    for i in $PRODUCER_INDEXES; do
        NODE=$(node_name $i)
        rsync -aW $TEST_BASE/ $NODE:/root/testHW/
        COMMAND="$(cat << EOF
        (   cd /lustre/testHW && \
            nohup /root/testHW/doMergingFromList_ForTests.py \
                --expectedBUs=1 \
                --option=2 \
                --paths_to_watch="/lustre/testHW/unmergedMON/Run*" \
                --bu=$i \
        )   >& /root/testHW/merger_opt1_${i}.log &
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        ssh $NODE "$COMMAND"
    done
    echo
} # launch_mergers_2

#-------------------------------------------------------------------------------
# Expects the config file name as the first argument, use mergeConfigForReal
# as default
function launch_producers {
    CONFIG=${1:-mergeConfigForReal}
    for i in $PRODUCER_INDEXES; do
        NODE=$(node_name $i)
        rsync -aW $TEST_BASE/ $NODE:/root/testHW/
        COMMAND="$(cat << EOF
        nohup /root/testHW/manageStreams.py \
            --config /root/testHW/$CONFIG \
            --bu $i \
            -i /root/testHW/frozen/ \
            -p  /lustre/testHW/ \
            --lumi-length-mean="$LUMI_LENGTH_MEAN" \
            --lumi-length-sigma="$LUMI_LENGTH_SIGMA" \
            >& /root/testHW/producer_${CONFIG}_${i}.log &
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        ssh $NODE $COMMAND
    done
    echo
} # launch_proudcers


#-------------------------------------------------------------------------------
function launch_producers_A {
    launch_producers_A mergeConfigForReal_A
} # launch_proudcers_A


#-------------------------------------------------------------------------------
function launch_simple_cat_A {
    ## The number of process per node is passed as the first arg, default=1
    PROCESSES_PER_NODE=${1:-1}
    RUN=500
    STREAM=A
    SOURCE_BASE=/lustre/testHW/unmergedDATA/Run${RUN}
    DESTINATION_BASE=/lustre/testHW/merged/Run${RUN}
    PERIOD=$PROCESSES_PER_NODE
    for i in $SIMPLE_CAT_A_INDEXES; do
        NODE=$(node_name $i)
        COMMAND="$(cat << EOF
        for j in {1..$PROCESSES_PER_NODE}; do\
            ((LS=$PERIOD*($i-1)+j-1));\
            SOURCES="$SOURCE_BASE/Data.${RUN}.LS\${LS}.Stream${STREAM}.*.raw";\
            DESTINATION=$DESTINATION_BASE/Data.${RUN}.LS\${LS}.Stream${STREAM}.raw;\
            LOG=/lustre/testHW/cat_\${LS}.log;\
            (time cat \$SOURCES > \$DESTINATION) >& \$LOG &\
        done
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        ssh $NODE "$COMMAND" &
    done
    echo
} # launch_simple_cat_A

#-------------------------------------------------------------------------------
function delete_previous_runs {
    echo "Deleting /lustre/testHW/{merged,unmerged*} ..."
    rm -rf /lustre/testHW/{merged,unmerged*}
    echo "    ... done."
    echo
} # delete_previous_runs

launch_main
