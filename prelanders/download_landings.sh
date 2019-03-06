#!/bin/bash
while IFS=: read -r url
do
    echo URL $url
    dirname=`echo $url | cut -d "?" -f1 | awk -F "pro/" '{print $2}'`
    mkdir -p $dirname
    echo DIRNAME $dirname
    httrack --sockets=32 --structure=100 $url -O $dirname > $dirname/httrack_command_log.txt
done <"urls.txt"
