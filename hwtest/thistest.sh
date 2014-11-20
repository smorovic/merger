## Source this file to setup the environment to use the version of the hardware 
## merger test from the folder holding this file

## Get the full path the folder where this file is located
## http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
## USAGE:
##    . ./thistest.sh [TEST_BASE]
echo "Setting up environment and inputs for a merger test ..."

## Get the directory containing this script
SOURCE="${BASH_SOURCE[0]}"
## Resolve $SOURCE until the file is no longer a symlink
while [ -h "$SOURCE" ]; do
  DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"
  SOURCE="$(readlink "$SOURCE")"
  ## If $SOURCE was a relative symlink, we need to resolve it relative to the
  ## path where the symlink file was located
  [[ $SOURCE != /* ]] && SOURCE="$DIR/$SOURCE" 
done
DIR="$( cd -P "$( dirname "$SOURCE" )" && pwd )"

export MERGER_BASE="$( dirname $DIR )"
echo "Using MERGER_BASE=$MERGER_BASE for the merger sources."

echo "Adding $MERGER_BASE to PATH ..."
export PATH=${PATH}:$MERGER_BASE

export TEST_BASE=${1:-"$( dirname $MERGER_BASE )"}
echo "Using TEST_BASE=$TEST_BASE for data."

## Will contain the inputs for the generation of the unmerged data
export FROZEN_BASE=$TEST_BASE/data/frozen
## Will contain unmerged data used as mini-merger inputs
export UNMERGED_BASE=$TEST_BASE/data/input
## Will contain mini-merger outputs used as macro-merger outputs
export MERGED1_BASE=$TEST_BASE/data/mergeBU_TEST
## Will contain macro-merger output
export MERGED2_BASE=$TEST_BASE/data/mergeMacro_TEST

echo "Updating $MERGER_BASE/dataFlowMergerMini.conf ..."
cat > $MERGER_BASE/dataFlowMergerMini.conf <<END_OF_HERE_DOC
[Input]
dataPath = "$UNMERGED_BASE/unmergedDATA/run300"
eolPath  = "$UNMERGED_BASE/unmergedMON"

[Output]
dataPath      = "$MERGED1_BASE"
smPath        = "$MERGED2_BASE"
dqmPath       = "$MERGED2_BASE"
ecalPath      = "$MERGED2_BASE"

[Misc]
mergeType     = "mini"
streamType    = "0"
deleteFiles   = "True"
debugLevel    = "1"
logConfigFile = "$MERGER_BASE/logFormat.conf"
mergeOption   = "optionC"
esServerUrl   = ""
esIndexName   = ""
numberOfShards = "1"
numberOfReplicas = "0"
END_OF_HERE_DOC

echo "Updating $MERGER_BASE/dataFlowMergerMacro.conf ..."
cat > $MERGER_BASE/dataFlowMergerMacro.conf <<END_OF_HERE_DOC
[Input]
dataPath = "$MERGED1_BASE/run300"
eolPath  = "$MERGED1_BASE"

[Output]
dataPath      = "$MERGED2_BASE"
smPath        = "$MERGED2_BASE"
dqmPath       = "$MERGED2_BASE"
ecalPath      = "$MERGED2_BASE"

[Misc]
mergeType     = "macro"
streamType    = "0"
deleteFiles   = "True"
debugLevel    = "1"
logConfigFile = "$MERGER_BASE/logFormat.conf"
mergeOption   = "optionC"
esServerUrl   = ""
esIndexName   = ""
numberOfShards = "1"
numberOfReplicas = "0"
END_OF_HERE_DOC

OLD=/opt/merger/dataFlowMerger.conf
NEW=$MERGER_BASE/dataFlowMergerMini.conf
echo "Changing $OLD to $NEW in Logging.py ..."
sed -i "s|$OLD|$NEW|" $MERGER_BASE/Logging.py

OLD=/var/log/merger.log
NEW=$MERGER_BASE/merger.log
echo "Changing $OLD to $NEW in logFormat.conf ..."
sed -i "s|$OLD|$NEW|" $MERGER_BASE/logFormat.conf

OLD=dataFlowMergerMini.conf
NEW=$MERGER_BASE/dataFlowMergerMini.conf
echo "Changing $OLD to $NEW in dataFlowMiniMergerInLine ..."
sed -i "s|$OLD|$NEW|" $MERGER_BASE/dataFlowMiniMergerInLine

OLD=dataFlowMergerMacro.conf
NEW=$MERGER_BASE/dataFlowMergerMacro.conf
echo "Changing $OLD to $NEW in dataFlowMacroMergerInLine ..."
sed -i "s|$OLD|$NEW|" $MERGER_BASE/dataFlowMacroMergerInLine

echo "Creating input files for the mini-merger input generation ..."
COMMAND="$MERGER_BASE/hwtest/makeInputFiles.py -p $FROZEN_BASE"
echo $COMMAND
eval $COMMAND

echo "Generating unmerged data ..."
$MERGER_BASE/hwtest/manageStreams.py \
    --config $MERGER_BASE/hwtest/mergeConfigTest \
    --bu 30 \
    -i $FROZEN_BASE \
    -p "$UNMERGED_BASE/"

echo "Do dataFlowMiniMergerInLine to run the mini-merger (Ctrl-C to exit)."
echo "Do dataFlowMacroMergerInLine to run the macro-merger (Ctrl-C to exit)."

