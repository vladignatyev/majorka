#!/bin/bash
while IFS=: read -r url
do
    echo URL $url
    dirname=`echo $url | cut -d "?" -f1 | awk -F "pro/" '{print $2}'`
    mkdir -p landers/$dirname
    echo DIRNAME landers/$dirname
    # httrack --sockets=32 --structure=100 $url -O $dirname > landers/$dirname/httrack_command_log.txt
    httrack --sockets=64 --structure=100 $url -O landers/$dirname > /dev/null
done <"urls.txt"
