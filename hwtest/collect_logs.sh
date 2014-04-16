#!/bin/bash
NAME=logs_v10.2
NODE_INDEXES="$(echo {1..10} {12..16})"
#NODE_INDEXES="$(echo {1..2})"
#MERGER_INDEXES="13 14"
MERGER_INDEXES=""

#______________________________________________________________________________
function node_name {
    ## Echo the name of the node given it's index
    echo wbua-TME-ComputeNode$1
} # node_name

#______________________________________________________________________________
mkdir /lustre/$NAME

## Copy code
rsync -aW /root/merger/hwtest /lustre/$NAME

#______________________________________________________________________________
## Kill the mergers
for i in $MERGER_INDEXES; do
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

#______________________________________________________________________________
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

#______________________________________________________________________________
# Make list of files in the relevant folders
pushd /lustre/testHW
for DIR in *merged*; do
    for RUN in Run300 Run500; do
        ls -ll $DIR/$RUN >& /lustre/$NAME/${DIR}_${RUN}_list.dat &
    done
done
popd
jobs

git tag netapp$(echo $NAME | sed 's/logs//')
git log | head -n 6 > /lustre/$NAME/README
vim /lustre/$NAME/README
