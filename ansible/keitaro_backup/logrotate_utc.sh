#! /bin/sh
TZ=UTC
export TZ
PATH=/bin:/usr/bin:/sbin:/usr/sbin
export PATH
logrotate -s /home/nep/logrotate-state /home/nep/logrotate.conf
exit $?
