#!/bin/bash
while IFS=: read -r url
do
    echo URL $url
    dirname=`echo $url | cut -d "?" -f1 | awk -F "pro/" '{print $2}'`
    echo DIRNAME landers/$dirname
    html-beautify -r -f landers/$dirname/index.html
done <"urls.txt"
