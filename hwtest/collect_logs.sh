#!/bin/bash
NAME=logsGui_v1
LIST_PRODUCERS=listProducers.txt
ROOT_LOCATION=/root/testHWGui/merger

rm -rf /lustre/$NAME;

## Copy the logs from each node
for NODE in `cat $LIST_PRODUCERS | cut -d' ' -f1`; do
    echo $NODE
    DESTINATION_DIR=/lustre/$NAME/$NODE
    COMMAND=$(cat <<EOF
        mkdir -p $DESTINATION_DIR; \
        mv $ROOT_LOCATION/hwtest/merger_*log $ROOT_LOCATION/hwtest/producer_*log $DESTINATION_DIR/; \
	rm -rf $ROOT_LOCATION/hwtest/$NODE;
EOF
    )
    echo "$COMMAND"
    ssh $NODE "$COMMAND"
done
