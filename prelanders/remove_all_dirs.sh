#!/bin/bash
while IFS=: read -r url
do
    echo URL $url
    dirname=`echo $url | cut -d "?" -f1 | awk -F "pro/" '{print $2}'`
    rm -rf landers/$dirname
    echo DIRNANE landers/$dirname
done <"urls.txt"
