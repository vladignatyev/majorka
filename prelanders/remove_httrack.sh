#!/bin/bash
while IFS=: read -r url
do
    echo URL $url
    dirname=`echo $url | cut -d "?" -f1 | awk -F "pro/" '{print $2}'`
    echo DIRNAME $dirname
    rm -rf $dirname/httrack_command_log.txt
    rm -rf $dirname/*.readme
    mv $dirname/*.html $dirname/index.html
done <"urls.txt"
