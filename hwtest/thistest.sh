## Source this file to setup the environment to use the version of the hardware 
## merger test from the folder holding this file

## Get the full path the folder where this file is located
## http://stackoverflow.com/questions/59895/can-a-bash-script-tell-what-directory-its-stored-in
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

echo "Appending \`$DIR' to \$PATH ..."
export PATH=${PATH}:$DIR
