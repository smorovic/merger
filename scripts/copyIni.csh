#!/bin/tcsh -f

# make ini files for all bus in case a problem has happened for a given run

setenv RUN $1

cat > list_ini.txt <<EOF
bu-c2d31-10-01
bu-c2d31-20-01
bu-c2d31-30-01
bu-c2d32-10-01
bu-c2d32-20-01
bu-c2d32-30-01
bu-c2d33-10-01
bu-c2d33-20-01
bu-c2d33-30-01
bu-c2d34-10-01
bu-c2d34-20-01
bu-c2d34-30-01
bu-c2d35-10-01
bu-c2d35-20-01
bu-c2d35-30-01
bu-c2d36-10-01
bu-c2d36-20-01
bu-c2d36-30-01
bu-c2d37-10-01
bu-c2d37-20-01
bu-c2d37-30-01
bu-c2d38-10-01
bu-c2d38-20-01
bu-c2d38-30-01
bu-c2d41-10-01
bu-c2d41-20-01
bu-c2d41-30-01
bu-c2d42-10-01
bu-c2d42-20-01
bu-c2d42-30-01
bu-c2e18-09-01
bu-c2e18-11-01
bu-c2e18-13-01
bu-c2e18-17-01
bu-c2e18-19-01
bu-c2e18-21-01
bu-c2e18-23-01
bu-c2e18-25-01
bu-c2e18-27-01
bu-c2e18-29-01
bu-c2e18-31-01
bu-c2e18-35-01
bu-c2e18-37-01
bu-c2e18-39-01
bu-c2e18-41-01
bu-c2e18-43-01
bu-c2f16-09-01
bu-c2f16-11-01
bu-c2f16-13-01
bu-c2f16-17-01
bu-c2f16-19-01
bu-c2f16-21-01
bu-c2f16-23-01
bu-c2f16-25-01
bu-c2f16-27-01
bu-c2f16-29-01
bu-c2f16-31-01
bu-c2f16-35-01
bu-c2f16-37-01
bu-c2f16-39-01
bu-c2f16-41-01
bu-c2f16-43-01
EOF

setenv INPUTDIR /store/lustre/mergeMacro/${RUN};
foreach BU (`cat list_ini.txt`)
cp $INPUTDIR/${RUN}_ls0000_streamALCALUMIPIXELS_bu-c2e18-37-01.ini     $INPUTDIR/${RUN}_ls0000_streamALCALUMIPIXELS_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamALCAPHISYM_bu-c2e18-37-01.ini         $INPUTDIR/${RUN}_ls0000_streamALCAPHISYM_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamCalibration_bu-c2e18-37-01.ini        $INPUTDIR/${RUN}_ls0000_streamCalibration_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamDQM_bu-c2e18-37-01.ini                $INPUTDIR/${RUN}_ls0000_streamDQM_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamDQMCalibration_bu-c2e18-37-01.ini     $INPUTDIR/${RUN}_ls0000_streamDQMCalibration_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamDQMEventDisplay_bu-c2e18-37-01.ini    $INPUTDIR/${RUN}_ls0000_streamDQMEventDisplay_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamDQMHistograms_bu-c2e18-37-01.ini      $INPUTDIR/${RUN}_ls0000_streamDQMHistograms_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamEcalCalibration_bu-c2e18-37-01.ini    $INPUTDIR/${RUN}_ls0000_streamEcalCalibration_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamError_bu-c2e18-37-01.ini              $INPUTDIR/${RUN}_ls0000_streamError_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamExpressCosmics_bu-c2e18-37-01.ini     $INPUTDIR/${RUN}_ls0000_streamExpressCosmics_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamHLTRates_bu-c2e18-37-01.ini           $INPUTDIR/${RUN}_ls0000_streamHLTRates_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamL1Rates_bu-c2e18-37-01.ini            $INPUTDIR/${RUN}_ls0000_streamL1Rates_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamLookArea_bu-c2e18-37-01.ini           $INPUTDIR/${RUN}_ls0000_streamLookArea_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamNanoDST_bu-c2e18-37-01.ini            $INPUTDIR/${RUN}_ls0000_streamNanoDST_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamPhysics_bu-c2e18-37-01.ini            $INPUTDIR/${RUN}_ls0000_streamPhysics_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamRPCMON_bu-c2e18-37-01.ini             $INPUTDIR/${RUN}_ls0000_streamRPCMON_${BU}.ini
cp $INPUTDIR/${RUN}_ls0000_streamTrackerCalibration_bu-c2e18-37-01.ini $INPUTDIR/${RUN}_ls0000_streamTrackerCalibration_${BU}.ini
end

rm -f list_ini.txt;
