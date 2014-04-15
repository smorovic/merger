## Source this script to launch the merging test

## Indexes of all the nodes to be used
#NODE_INDEXES="$( echo {1..10} {12..14} 16)"
NODE_INDEXES="$(echo {1..9} 12 16)"
MERGER1_INDEXES="$(echo {1..9} 12)"
MERGER_INDEX="13"
MERGERA_INDEX="14"
PRODUCER_INDEXES="$(echo {1..9} 12)"
SIMPLE_CAT_A_INDEXES="$(echo {1..10} {12..14} 16)"
# PRODUCER_INDEXES="$(echo {1..2})"
TEST_BASE=/root/merger/hwtest

## defines node_name, count_args
source $TEST_BASE/tools.sh

#-------------------------------------------------------------------------------
function launch_merger_0 {
    for i in $MERGER_INDEX; do
        NODE=$(node_name $i)
        rsync -aW $TEST_BASE/ $NODE:/root/testHW/
        COMMAND="$(cat << EOF
        (   cd /lustre/testHW ; \
            nohup /root/testHW/doMergingFromList_ForTests.py \
                --expectedBUs=$(count_args $PRODUCER_INDEXES) \
                --option=0 \
                --paths_to_watch="/lustre/testHW/unmergedMON/Run300" \
        )   >& /root/testHW/merger_opt0_${i}.log &
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        ssh $NODE $COMMAND
    done
    echo
} # launch_merger_0


#-------------------------------------------------------------------------------
function launch_mergerA_0 {
    for i in $MERGERA_INDEX; do
        NODE=$(node_name $i)
        rsync -aW $TEST_BASE/ $NODE:/root/testHW/
        COMMAND="$(cat << EOF
        (   cd /lustre/testHW ; \
            nohup /root/testHW/doMergingFromList_ForTests.py \
                --expectedBUs=$(count_args $PRODUCER_INDEXES) \
                --option=0 \
                --paths_to_watch="/lustre/testHW/unmergedMON/Run500" \
        )   >& /root/testHW/mergerA_opt0_${i}.log &
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        ssh $NODE $COMMAND
    done
    echo
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
function launch_producers {
    for i in $PRODUCER_INDEXES; do
        NODE=$(node_name $i)
        CONFIG=mergeConfigForReal
        rsync -aW $TEST_BASE/ $NODE:/root/testHW/
        COMMAND="$(cat << EOF
        nohup /root/testHW/manageStreams.py \
            --config /root/testHW/$CONFIG \
            --bu $i \
            -i /root/testHW/frozen/ \
            -p  /lustre/testHW/ \
            >& /root/testHW/producer${i}.log &
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
    for i in $PRODUCER_INDEXES; do
        NODE=$(node_name $i)
        CONFIG=mergeConfigForReal_A
        rsync -aW $TEST_BASE/ $NODE:/root/testHW/
        COMMAND="$(cat << EOF
        nohup /root/testHW/manageStreams.py \
            --config /root/testHW/$CONFIG \
            --bu $i \
            -i /root/testHW/frozen/ \
            -p  /lustre/testHW/ \
            >& /root/testHW/producer${i}_A.log &
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        ssh $NODE $COMMAND
    done
    echo
} # launch_proudcers_A


#-------------------------------------------------------------------------------
function launch_simple_cat_A {
    ## The number of process per node is passed as the first arg, default=1
    PROCESSES_PER_NODE=${1:-1}
    BASE=/lustre/testHW/unmergedDATA/Run500
    PERIOD=$(count_args $SIMPLE_CAT_A_INDEXES)
    for i in $SIMPLE_CAT_A_INDEXES; do
        NODE=$(node_name $i)
        COMMAND="$(cat << EOF
        for j in {1..$PROCESSES_PER_NODE}; do\
            ((LS=i+(j-1)*PERIOD));\
            SOURCES="$BASE/Data.500.LS${LS}.StreamA.*.raw";\
            DESTINATION=$BASE/Data.500.LS${LS}.StreamA.raw;\
            LOG=/lustre/testHW/cat_${LS}.log;\
            (time cat \$SOURCES > \$DESTINATION) >& \$LOG &\
        done
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        #ssh $NODE "$COMMAND"
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

# delete_previous_runs
# launch_mergers_1
# launch_merger_0
# launch_mergerA_0
# launch_producers
# launch_producers_A
launch_simple_cat_A 2

#merge option 2
#launch_mergers_2
#launch_producers
