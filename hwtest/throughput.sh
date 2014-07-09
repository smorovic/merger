source tools.sh
RUN=200
BANDWIDTH_IN_GB=$(du -sk /lustre/testHW/mergerMacro/run${RUN} -B 1G | awk '{print $1}')
START=$(grep writing /lustre/$NAME/*/prod*run${RUN}*.log |\
            head -1 | sort | head -1 |\
            awk -F: '{printf "%s:%s:%s", $2, $3, $4}')
END=$(grep Time /lustre/$NAME/*/merg*run${RUN}*_macro.log |\
            tail -n1 | sort | tail -n1 |\
            awk -F, '{print $1}')
echo "Volume (GB): $(quote "$BANDWIDTH_IN_GB")"
echo "Start (hh:mm:ss): $(quote "$START")"
echo "End (hh:mm:ss): $(quote "$END")"
./throughput.py $BANDWIDTH_IN_GB $START $END
