## Password-less access
eval `ssh-agent`
ssh-add

## Launch a test (on node 10 in /root/merger/hwtest)
. launch.sh

## Bring the functions defined in tools.sh into your name space
. tools.sh

## Watch the input and output folders on lustre to be
## deleted, created, and filled (on node 10)
INPUT_BASE=/lustre/cern/data
watch "du -sk $INPUT_BASE/{,*/}*merge*/run* 2>/dev/null"

## Watch a merger start up (and get killed, on nodes 13, 14)
watch "echo $HOSTNAME; ps awwx | grep doMerging | grep -v grep"

## Watch producers start up, spawn sub-processes, and close (on nodes 1, 2)
watch "echo $HOSTNAME; ps awwx | grep manageStreams | grep -v grep"

## Watch both mergers and producers
watch "echo $HOSTNAME; ps awwx | egrep 'dataFlowMergerInLine|manageStreams' | grep -v grep"

## Check that ther are no input files left (on node 10)
ls /lustre/cern/data/unmerged*/*

## Define a list of nodes
NODES="$(parse_machine_list all_nodes.txt)"

## Add a missing frozen input
SIZES="10 20 30 40 50 100 200 300 400 500"
FROZEN_BASE=/home/cern/frozen
for n in $NODES; do
    echo $n
    ssh $n "mkdir -p $FROZEN_BASE"
    for MB in $SIZES; do
        ssh $n "OF=$FROZEN_BASE/inputFile_${MB}MB.dat; dd if=/dev/zero of=\$OF bs=1M count=$MB; echo >> \$OF"
    done
done

for MB in 10 20 30 40 50 100 200 300 400 500; do OF=/root/testHW/frozen/inputFile_${MB}MB.dat; dd if=/dev/zero of=$OF bs=1M count=$MB; echo >> $OF; done

## Setup RAM disks
source tools.sh
FROZEN_BASE=/ramdisk/frozen
INPUT_BASE=/ramdisk/data # RAM disk
SIZES="10 20 30 40 50 100 200"
# COMMAND="mkdir /ramdisk; mount -o size=1G -t tmpfs none /ramdisk; df -h /ramdisk"
COMMAND="mkdir -p $FROZEN_BASE; for MB in $SIZES; do OF=$FROZEN_BASE/inputFile_\${MB}MB.dat; dd if=/dev/zero of=\$OF bs=1M count=\$MB; echo >> \$OF; done"
COMMAND="chmod 755 $FROZEN_BASE; chmod 777 $INPUT_BASE"
for NODE in $(parse_machine_list all_nodes.txt); do
    echo_and_ssh $NODE "$COMMAND"
done

## Screen
C-a : multiuser on
C-a : acladd cern


## Setup a DDN node
ssh root@$NODE

## Add user CERN with the ID 500
useradd cern -u500
# grep 500 /etc/passwd
# userdel test
# rm -rf /home/test /var/mail/test
passwd cern
yum -y install rsync ntp

## Sync the date and time with the NTP server
ntpdate 129.6.15.28

## Remount Lustre with the flock option to enable the POSIX file locking
umount /lustre
mount -t lustre -o flock 192.168.110.72@o2ib:/scratch /lustre ## DDN

## Setup RAM disk
mkdir /ramdisk
mount -o size=1G -t tmpfs none /ramdisk
df -h /ramdisk

exit

## Copy the public key for passwordless ssh
ssh-copy-id $NODE
ssh $NODE
## Make frozen inputs
SIZES="10 20 30 40 50 100 200"
FROZEN_BASE=/home/cern/frozen
mkdir -p $FROZEN_BASE
for MB in $SIZES; do
    OF=$FROZEN_BASE/inputFile_${MB}MB.dat
    dd if=/dev/zero of=$OF bs=1M count=$MB
    echo >> $OF
done

FROZEN_BASE=/ramdisk/frozen
mkdir -p $FROZEN_BASE
for MB in $SIZES; do
    OF=$FROZEN_BASE/inputFile_${MB}MB.dat
    dd if=/dev/zero of=$OF bs=1M count=$MB
    echo >> $OF
done

exit

## DDN Duesseldorf Lab Monitoring
ssh user:user@192.168.2.141
show vd count rate +2
