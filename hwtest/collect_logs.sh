#!/bin/bash
source tools.sh
NAME=logs_v0.6
NODES=$(parse_machine_list all_nodes.txt)
MASTER_BASE=/home/cern/merger
SLAVE_BASE=/home/cern/slave
OUTPUT_BASE=/lustre/cern/data
LOGS_BASE=/lustre/cern/logs/$NAME

#______________________________________________________________________________
mkdir $LOGS_BASE

## Copy code
rsync -aW --exclude='.git' $MASTER_BASE $LOGS_BASE
#rm -rf $(find $LOGS_BASE -type d -name '.git')

#______________________________________________________________________________
## Copy the logs from each node
for NODE in $NODES; do 
    echo $NODE
    DESTINATION_DIR=$LOGS_BASE
    LOGS_MASK="\$(find $SLAVE_BASE -name '*.log')"
    COMMAND=$(cat <<EOF
        mkdir -p $DESTINATION_DIR; \
        cp $LOGS_MASK $DESTINATION_DIR;
EOF
    )
    echo_and_ssh $NODE "$COMMAND"
done

#______________________________________________________________________________
# Make list of files in the relevant folders
pushd $OUTPUT_BASE
for DIR in merger*; do
    pushd $DIR
    for RUN in run*; do
        ls -ll $RUN >& $LOGS_BASE/${DIR}_${RUN}_list.dat &
        ls -ll $RUN/open >& $LOGS_BASE/${DIR}_${RUN}_open_list.dat &
    done
    popd
done

popd
jobs

# git tag netapp$(echo $NAME | sed 's/logs//')
# git log | head -n 6 > /lustre/$NAME/README

