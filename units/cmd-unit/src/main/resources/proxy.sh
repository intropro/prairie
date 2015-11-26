#!/bin/bash
host=%s
port=%d
if $1 ; then
    cat -
else
    echo ''
fi | java -cp %s com.intropro.prairie.unit.cmd.CommandClient -h ${host} -p ${port} -c $2 ${@:3}