#______________________________________________________________________________
function node_name {
    ## Echo the name of the node given it's index
    echo wbua-TME-ComputeNode$1
} # node_name


#______________________________________________________________________________
function count_args {
    echo $@ | wc -w
} # count_args


#______________________________________________________________________________
function parse_machine_list {
    ## sed removes Bash/Python-style comments starting with `#'
    ## awk makes sure to ignore white space around the node name
    echo "$(sed 's/#.*$//' $1 | awk '{print $1}')"
} ## parse_machine_list


#______________________________________________________________________________
function echo_and_ssh {
    NODE=$1
    COMMAND="$2"
    LAUNCH_IN_THE_BACKGROUND=${1:-0}
    echo "+++ $NODE"
    ## Format the command for printing, add more line breaks.
    FORMATTED_COMMAND="$(echo $COMMAND |\
                         tr ';' '\n' |\
                         sed -e 's/ -/ \\\n    -/g' -e 's/ >/ \\\n    >/g' |\
                         sed -E 's/^/    /g')"
    echo "$FORMATTED_COMMAND"
    if [[ $LAUNCH_IN_THE_BACKGROUND == "1" ]]; then
        ssh $NODE "$COMMAND" &
    else
        ssh $NODE "$COMMAND"
    fi
}  ## echo_and_ssh

