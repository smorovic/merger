#______________________________________________________________________________
function node_name {
    ## Echo the name of the node given it's index
    echo wbua-TME-ComputeNode$1
} # node_name


#______________________________________________________________________________
function count_args {
    echo $@ | wc -w
} # count_args
