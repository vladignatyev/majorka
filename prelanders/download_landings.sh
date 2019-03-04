#!/bin/bash
while IFS=: read -r url	
do
    echo URL $url
    dirname=`echo $url | cut -d "?" -f1 | awk -F "pro/" '{print $2}'` 
    mkdir -p $dirname
    echo DIRNANE $dirname
    httrack -O $dirname $url > $dirname/httrack_command_log.txt
done <"urls_github.txt"
