#!/bin/bash
host=%s
port=%d
if $1 ; then
    { echo ${@:2} ; cat - ; }
else
    echo ${@:2}
fi  | nc ${host} ${port} | {
    while read line; do
        echo "$line"
    done
    exit ${line};
}
