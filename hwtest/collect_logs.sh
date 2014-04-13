#!/bin/bash
NAME=logs_v7.6
NODE_INDEXES="$(echo {1..9} {12..14})"
#NODE_INDEXES="$(echo {1..2})"

mkdir /lustre/$NAME

## Copy code
rsync -aW /root/merger/hwtest /lustre/$NAME

## Copy the logs from each node
for i in $NODE_INDEXES; do 
    NODE=wbua-TME-ComputeNode$i
    echo $NODE
    DESTINATION_DIR=/lustre/$NAME/$NODE
    LOGS_MASK='/root/testHW/*.log'
    MEMINFO_MASK='/var/log/meminfo*.*'
    COMMAND=$(cat <<EOF
        mkdir $DESTINATION_DIR; \
        mv $LOGS_MASK $DESTINATION_DIR; \
        mv $MEMINFO_MASK $DESTINATION_DIR
EOF
    )
    echo "$COMMAND"
    ssh $NODE "$COMMAND"
done

# Make list of files in the relevant folders
pushd /lustre/testHW
for DIR in *merged*; do
    for RUN in Run300 Run500; do
        ls $DIR/$RUN >& /lustre/$NAME/${DIR}_${RUN}_list.dat &
    done
done
popd
jobs

