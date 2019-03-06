#!/bin/bash
while IFS=: read -r url
do
    echo URL $url
    dirname=`echo $url | cut -d "?" -f1 | awk -F "pro/" '{print $2}'`
    echo DIRNAME landers/$dirname
    rm -rf landers/$dirname/httrack_command_log.txt
    rm -rf landers/$dirname/cookies.txt
    rm -rf landers/$dirname/backblue.gif
    rm -rf landers/$dirname/fade.gif
    rm -rf landers/$dirname/GET.html
    rm -rf landers/$dirname/GET
    rm -rf landers/$dirname/test
    rm -rf landers/$dirname/test.html
    rm -rf landers/$dirname/hts*
    rm -rf landers/$dirname/OPR

    mv landers/$dirname/$dirname*.html landers/$dirname/index.html
done <"urls.txt"
