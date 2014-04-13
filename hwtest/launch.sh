## Source this script to launch the merging test

## Endexes of all the nodes to be used
#NODE_INDEXES="$( echo {1..10} {12..14} 16)"
NODE_INDEXES="$(echo {1..9} 12 16)"
MERGER1_INDEXES="$(echo {1..9})"
MERGER_INDEX="13"
MERGERA_INDEX="14"
PRODUCER_INDEXES="$(echo {1..9} 12)"
# PRODUCER_INDEXES="$(echo {1..2})"
TEST_BASE=/root/merger/hwtest

#-------------------------------------------------------------------------------
function launch_merger_0 {
    for i in $MERGER_INDEX; do
        NODE=wbua-TME-ComputeNode$i
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
        NODE=wbua-TME-ComputeNode$i
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
        NODE=wbua-TME-ComputeNode$i
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
function launch_producers {
    for i in $PRODUCER_INDEXES; do
        NODE=wbua-TME-ComputeNode$i
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
        NODE=wbua-TME-ComputeNode$i
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
launch_producers_A
echo

