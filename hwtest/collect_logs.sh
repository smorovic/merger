#!/bin/bash
NAME=logs_v11.18
NODES=$(parse_machine_list all_nodes.txt)

#______________________________________________________________________________
mkdir /lustre/$NAME

## Copy code
rsync -aW /root/veverka/merger /lustre/$NAME
rm -rf /lustre/$NAME/merger/.git

#______________________________________________________________________________
## Copy the logs from each node
for NODE in $NODES; do 
    echo $NODE
    DESTINATION_DIR=/lustre/$NAME/$NODE
    LOGS_MASK='/root/testHW/python/*.log'
    COMMAND=$(cat <<EOF
        mkdir $DESTINATION_DIR; \
        cp $LOGS_MASK $DESTINATION_DIR;
EOF
    )
    echo_and_ssh $NODE "$COMMAND"
done

#______________________________________________________________________________
# Make list of files in the relevant folders
pushd /lustre/testHW
for DIR in merger*; do
    pushd $DIR
    for RUN in run*; do
        ls -ll $RUN >& /lustre/$NAME/${DIR}_${RUN}_list.dat &
        ls -ll $RUN/open >& /lustre/$NAME/${DIR}_${RUN}_open_list.dat &
    done
    popd
done

popd
jobs

git tag netapp$(echo $NAME | sed 's/logs//')
git log | head -n 6 > /lustre/$NAME/README
vim /lustre/$NAME/README
