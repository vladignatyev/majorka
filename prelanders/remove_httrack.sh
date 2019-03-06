#!/bin/bash
while IFS=: read -r url
do
    echo URL $url
    dirname=`echo $url | cut -d "?" -f1 | awk -F "pro/" '{print $2}'`
    echo DIRNAME $dirname
    rm -rf $dirname/httrack_command_log.txt
    rm -rf $dirname/cookies.txt
    rm -rf $dirname/backblue.gif
    rm -rf $dirname/fade.gif
    rm -rf $dirname/GET.html
    rm -rf $dirname/test.html
    rm -rf $dirname/hts*
    rm -rf $dirname/OPR

    mv $dirname/$dirname*.html $dirname/index.html
done <"urls.txt"
