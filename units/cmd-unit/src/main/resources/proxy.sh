#!/bin/bash
host=%s
port=%d
if $1 ; then
    { echo ${@:2} ; cat - ; }
else
    echo ${@:2}
fi  | nc ${host} ${port} | {
    while read line; do
        if [ "${line:0:47}" == "---this is the first line marker for prairie---" ]; then
            printf "%%s" "${line:47}"
        else
            printf "\n%%s" "${line}"
        fi
    done
    exit ${line};
}
