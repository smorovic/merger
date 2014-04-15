## Source this script to launch the merging test

## Indexes of all the nodes to be used
#NODE_INDEXES="$( echo {1..10} {12..14} 16)"
NODE_INDEXES="$(echo {1..9} 12 16)"
MERGER1_INDEXES="$(echo {1..9} 12)"
MERGER_INDEX="13"
MERGERA_INDEX="14"
PRODUCER_INDEXES="$(echo {1..9} 12)"
SIMPLE_CAT_A_INDEXES="$PRODUCER_INDEXES"
# PRODUCER_INDEXES="$(echo {1..2})"
TEST_BASE=/root/merger/hwtest

## defines node_name
source $TEST_BASE/tools.sh

#-------------------------------------------------------------------------------
function launch_merger_0 {
    for i in $MERGER_INDEX; do
        NODE=$(node_name $i)
        rsync -aW $TEST_BASE/ $NODE:/root/testHW/
        COMMAND="$(cat << EOF
        (   cd /lustre/testHW ; \
            nohup /root/testHW/doMergingFromList_ForTests.py \
                --expectedBUs=$(echo $PRODUCER_INDEXES | wc -w) \
                --option=0 \
                --paths_to_watch="/lustre/testHW/unmergedMON/Run300" \
        )   >& /root/testHW/merger_opt0_${i}.log &
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        ssh $NODE $COMMAND
    done
} # launch_merger_0


#-------------------------------------------------------------------------------
function launch_mergerA_0 {
    for i in $MERGERA_INDEX; do
        NODE=$(node_name $i)
        rsync -aW $TEST_BASE/ $NODE:/root/testHW/
        COMMAND="$(cat << EOF
        (   cd /lustre/testHW ; \
            nohup /root/testHW/doMergingFromList_ForTests.py \
                --expectedBUs=$(echo $PRODUCER_INDEXES | wc -w) \
                --option=0 \
                --paths_to_watch="/lustre/testHW/unmergedMON/Run500" \
        )   >& /root/testHW/mergerA_opt0_${i}.log &
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        ssh $NODE $COMMAND
    done
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
} # launch_proudcers_A


#-------------------------------------------------------------------------------
function launch_simple_cat_A {
    ## The number of process per node is passed as the first arg, default=1
    PROCESSES_PER_NODE=${1:-1}
    for i in $SIMPLE_CAT_A_INDEXES; do
        NODE=$(node_name $i)
        COMMAND="$(cat << EOF
        SOURCES="/lustre/testHW/unmergedDATA/Run500/Data.500.LS${i}.StreamA.*.raw";\
        DESTINATION=/lustre/testHW/merged/Run500/Data.500.LS${i}.StreamA.raw;\
        LOG=/lustre/testHW/cat_${i}.log;\
        (time cat \$SOURCES > \$DESTINATION) >& \$LOG &
EOF
        )"
        echo $NODE
        echo "    $COMMAND"
        #ssh $NODE "$COMMAND"
    done
# 1276  for i in {1..10} {12..14} 16; do NODE=wbua-TME-ComputeNode${i}; echo $NODE; ssh $NODE "( time cat  /lustre/testHW/unmergedDATA/Run500/Data.500.LS${i}.StreamA.*.raw > /lustre/testHW/merged/Run500/Data.500.LS${i}.StreamA.raw ) >& /lustre/testHW/cat_${i}.log & " & done
} # launch_simple_cat_A

echo "Deleting /lustre/testHW/{merged,unmerged*} ..."
rm -rf /lustre/testHW/{merged,unmerged*}
echo "    ... done."
echo
# launch_mergers_1
# echo
# launch_merger_0
# echo
launch_mergerA_0
echo
# launch_producers
# echo
# launch_producers_A
# echo
# launch_simple_cat_A
# echo

#merge option 2
#launch_mergers_2
#echo
#launch_producers
#echo
