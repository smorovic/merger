#!/bin/tcsh -f

# make ini files for all bus in case a problem has happened for a given run

setenv RUN $1

cat > list_ini.txt <<EOF
bu-c2e45-30-01
EOF

setenv INPUTDIR /store/lustre/mergeMacro/${RUN};
foreach BU (`cat list_ini.txt`)

cp $INPUTDIR/streamALCAELECTRON/data/${RUN}_ls0000_streamALCAELECTRON_bu-c2e45-10-01.ini                                          $INPUTDIR/streamALCAELECTRON/data/${RUN}_ls0000_streamALCAELECTRON_${BU}.ini
cp $INPUTDIR/streamALCALUMIPIXELS/data/${RUN}_ls0000_streamALCALUMIPIXELS_bu-c2e45-10-01.ini					  $INPUTDIR/streamALCALUMIPIXELS/data/${RUN}_ls0000_streamALCALUMIPIXELS_${BU}.ini
cp $INPUTDIR/streamALCAP0/data/${RUN}_ls0000_streamALCAP0_bu-c2e45-10-01.ini							  $INPUTDIR/streamALCAP0/data/${RUN}_ls0000_streamALCAP0_${BU}.ini
cp $INPUTDIR/streamALCAPHISYM/data/${RUN}_ls0000_streamALCAPHISYM_bu-c2e45-10-01.ini						  $INPUTDIR/streamALCAPHISYM/data/${RUN}_ls0000_streamALCAPHISYM_${BU}.ini
cp $INPUTDIR/streamCalibration/data/${RUN}_ls0000_streamCalibration_bu-c2e45-10-01.ini						  $INPUTDIR/streamCalibration/data/${RUN}_ls0000_streamCalibration_${BU}.ini
cp $INPUTDIR/streamDQMCalibration/data/${RUN}_ls0000_streamDQMCalibration_bu-c2e45-10-01.ini					  $INPUTDIR/streamDQMCalibration/data/${RUN}_ls0000_streamDQMCalibration_${BU}.ini
cp $INPUTDIR/streamDQM/data/${RUN}_ls0000_streamDQM_bu-c2e45-10-01.ini								  $INPUTDIR/streamDQM/data/${RUN}_ls0000_streamDQM_${BU}.ini
cp $INPUTDIR/streamDQMEventDisplay/data/${RUN}_ls0000_streamDQMEventDisplay_bu-c2e45-10-01.ini					  $INPUTDIR/streamDQMEventDisplay/data/${RUN}_ls0000_streamDQMEventDisplay_${BU}.ini
cp $INPUTDIR/streamDQMHistograms/data/${RUN}_ls0000_streamDQMHistograms_bu-c2e45-10-01.ini					  $INPUTDIR/streamDQMHistograms/data/${RUN}_ls0000_streamDQMHistograms_${BU}.ini
cp $INPUTDIR/streamEcalCalibration/data/${RUN}_ls0000_streamEcalCalibration_bu-c2e45-10-01.ini					  $INPUTDIR/streamEcalCalibration/data/${RUN}_ls0000_streamEcalCalibration_${BU}.ini
cp $INPUTDIR/streamError/data/${RUN}_ls0000_streamError_bu-c2e45-10-01.ini							  $INPUTDIR/streamError/data/${RUN}_ls0000_streamError_${BU}.ini
cp $INPUTDIR/streamExpress/data/${RUN}_ls0000_streamExpress_bu-c2e45-10-01.ini							  $INPUTDIR/streamExpress/data/${RUN}_ls0000_streamExpress_${BU}.ini
cp $INPUTDIR/streamHLTMonitor/data/${RUN}_ls0000_streamHLTMonitor_bu-c2e45-10-01.ini						  $INPUTDIR/streamHLTMonitor/data/${RUN}_ls0000_streamHLTMonitor_${BU}.ini
cp $INPUTDIR/streamHLTRates/data/${RUN}_ls0000_streamHLTRates_bu-c2e45-10-01.ini						  $INPUTDIR/streamHLTRates/data/${RUN}_ls0000_streamHLTRates_${BU}.ini
cp $INPUTDIR/streamL1Rates/data/${RUN}_ls0000_streamL1Rates_bu-c2e45-10-01.ini							  $INPUTDIR/streamL1Rates/data/${RUN}_ls0000_streamL1Rates_${BU}.ini
cp $INPUTDIR/streamNanoDST/data/${RUN}_ls0000_streamNanoDST_bu-c2e45-10-01.ini							  $INPUTDIR/streamNanoDST/data/${RUN}_ls0000_streamNanoDST_${BU}.ini
cp $INPUTDIR/streamParking/data/${RUN}_ls0000_streamParking_bu-c2e45-10-01.ini							  $INPUTDIR/streamParking/data/${RUN}_ls0000_streamParking_${BU}.ini
cp $INPUTDIR/streamPhysicsEGammaCommissioning/data/${RUN}_ls0000_streamPhysicsEGammaCommissioning_bu-c2e45-10-01.ini		  $INPUTDIR/streamPhysicsEGammaCommissioning/data/${RUN}_ls0000_streamPhysicsEGammaCommissioning_${BU}.ini
cp $INPUTDIR/streamPhysicsEndOfFill/data/${RUN}_ls0000_streamPhysicsEndOfFill_bu-c2e45-10-01.ini				  $INPUTDIR/streamPhysicsEndOfFill/data/${RUN}_ls0000_streamPhysicsEndOfFill_${BU}.ini
cp $INPUTDIR/streamPhysicsHadronsTaus/data/${RUN}_ls0000_streamPhysicsHadronsTaus_bu-c2e45-10-01.ini				  $INPUTDIR/streamPhysicsHadronsTaus/data/${RUN}_ls0000_streamPhysicsHadronsTaus_${BU}.ini
cp $INPUTDIR/streamPhysicsMuons/data/${RUN}_ls0000_streamPhysicsMuons_bu-c2e45-10-01.ini					  $INPUTDIR/streamPhysicsMuons/data/${RUN}_ls0000_streamPhysicsMuons_${BU}.ini
cp $INPUTDIR/streamPhysicsParkingScoutingMonitor/data/${RUN}_ls0000_streamPhysicsParkingScoutingMonitor_bu-c2e45-10-01.ini	  $INPUTDIR/streamPhysicsParkingScoutingMonitor/data/${RUN}_ls0000_streamPhysicsParkingScoutingMonitor_${BU}.ini
cp $INPUTDIR/streamRPCMON/data/${RUN}_ls0000_streamRPCMON_bu-c2e45-10-01.ini							  $INPUTDIR/streamRPCMON/data/${RUN}_ls0000_streamRPCMON_${BU}.ini
cp $INPUTDIR/streamScoutingCalo/data/${RUN}_ls0000_streamScoutingCalo_bu-c2e45-10-01.ini					  $INPUTDIR/streamScoutingCalo/data/${RUN}_ls0000_streamScoutingCalo_${BU}.ini
cp $INPUTDIR/streamScoutingPF/data/${RUN}_ls0000_streamScoutingPF_bu-c2e45-10-01.ini						  $INPUTDIR/streamScoutingPF/data/${RUN}_ls0000_streamScoutingPF_${BU}.ini
end

rm -f list_ini.txt;
