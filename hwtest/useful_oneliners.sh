## Launch a test (on node 10 in /root/merger/hwtest)
. launch.sh

## Watch the input and output folders on lustre to be
## deleted, created, and filled (on node 10)
watch "du -sk /lustre/cern/data/{,*/}*merge*/run* 2>/dev/null"

## Watch a merger start up (and get killed, on nodes 13, 14)
watch "echo $HOSTNAME; ps awwx | grep doMerging | grep -v grep"

## Watch producers start up, spawn sub-processes, and close (on nodes 1, 2)
watch "echo $HOSTNAME; ps awwx | grep manageStreams | grep -v grep"

## Check that ther are no input files left (on node 10)
ls /lustre/cern/data/unmerged*/*

## Define a list of nodes
NODES=$(echo wbua-TME-ComputeNode{1..9} wbua-TME-ComputeNode{12..14} wbua-TME-ComputeNode16)

## Add a missing frozen input
for n in $NODES; do echo $n; ssh $n "mkdir -p /root/testHW/frozen"; for MB in 10 20 30 40 50 100 200 300 400 500; do ssh $n "OF=/root/testHW/frozen/inputFile_${MB}MB.dat; dd if=/dev/zero of=\$OF bs=1M count=$MB; echo >> \$OF"; done; done

for MB in 10 20 30 40 50 100 200 300 400 500; do OF=/root/testHW/frozen/inputFile_${MB}MB.dat; dd if=/dev/zero of=$OF bs=1M count=$MB; echo >> $OF; done

## Setup RAM disks
source tools.sh
for NODE in $(parse_machine_list all_nodes.txt); do
    # COMMAND="mkdir /ramdisk; mount -o size=1G -t tmpfs none /ramdisk; df -h /ramdisk"
    COMMAND="mkdir -p /ramdisk/testHW/frozen; for MB in 10 20 30 40 50 100 200; do OF=/ramdisk/testHW/frozen/inputFile_\${MB}MB.dat; dd if=/dev/zero of=\$OF bs=1M count=\$MB; echo >> $OF; done"
    echo_and_ssh $NODE "$COMMAND"
done
