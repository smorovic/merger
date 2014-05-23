source tools.sh
BANDWIDTH_IN_GB=$(du -sk /lustre/testHW/mergerMacro/ -B 1G | awk '{print $1}')
START=$(grep writing /lustre/$NAME/*/prod*.log |\
            head -1 | sort | head -1 |\
            awk -F: '{printf "%s:%s:%s", $2, $3, $4}')
END=$(grep Time /lustre/$NAME/*/merg*.log |\
            tail -n1 | sort | tail -n1 |\
            awk -F: '{printf "%s:%s:%s", $2, $3, $4}' |\
            awk -F, '{print $1}')
# quote "$BANDWIDTH_IN_GB"
# quote "$START"
# quote "$END"
./throughput.py $BANDWIDTH_IN_GB $START $END
