## Assume LOGS_BASE is defined from sourcing collect_logs.sh
for DIR in $OUTPUT_BASE/merger*; do
    DIR=$(basename $DIR)
    for RUN in $OUTPUT_BASE/$DIR/run*; do
        RUN=$(basename $RUN)
        CLOSED=$(grep dat $LOGS_BASE/${DIR}_${RUN}_list.dat | wc -l)
        OPENED=$(grep dat $LOGS_BASE/${DIR}_${RUN}_open_list.dat | wc -l)
        ((TOTAL=$OPENED+$CLOSED))
        if [[ "$TOTAL" -le "0" ]]; then
            continue
        fi
        ((CLOSED_PERCENT=100*$CLOSED/$TOTAL))
        ((OPENED_PERCENT=100-$CLOSED_PERCENT))
        echo "Total / closed / opened files:" \
            "$TOTAL / $CLOSED ($CLOSED_PERCENT%) /" \
            "$OPENED ($OPENED_PERCENT%)"
    done
done
