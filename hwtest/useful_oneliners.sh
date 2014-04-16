## Launch a test (on node 10 in /root/merger/hwtest)
. launch.sh

## Watch the input and output folders on lustre to be
## deleted, created, and filled (on node 10)
watch "du -sk /lustre/testHW/*merged*/Run* 2>/dev/null"

## Watch a merger start up (and get killed, on nodes 13, 14)
watch "echo $HOSTNAME; ps awwx | grep doMerging | grep -v grep"

## Watch producers start up, spawn sub-processes, and close (on nodes 1, 2)
watch "echo $HOSTNAME; ps awwx | grep manageStreams | grep -v grep"

## Check that ther are no input files left (on node 10)
ls /lustre/testHW/unmerged*/*

## Define a list of nodes
NODES=$(echo wbua-TME-ComputeNode{1..9} wbua-TME-ComputeNode{12..14} wbua-TME-ComputeNode16)

## Add a missing frozen input
MB=400; for n in $NODES; do echo $n; ssh $n "OF=/root/testHW/frozen/inputFile_${MB}MB.dat; dd if=/dev/zero of=$OF bs=1M count=$MB; echo >> $OF"; done

