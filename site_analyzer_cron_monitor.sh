# Author : Shreeshail G
# Created Date : 04 Oct 2017

SSH_AUTH_SOCK=/tmp/ssh-Y57qwnBgyqfV/agent.26342; export SSH_AUTH_SOCK;

#status_flag=`gwsh p3-analytics03.extranet.akamai.com "tail /a/logs/SAtool.log" > /tmp/test; cat /tmp/test | grep 'succeeded in sending to' > /tmp/test_status; ls -l /tmp/test_status | awk -F ' ' '{print $5}'`

status_flag=`/usr/local/bin/gwsh --legacy -i sgumgeri-deployed-2017-07-24.ppk p3-analytics03.extranet.akamai.com "tail /a/logs/SAtool.log" > /tmp/test; cat /tmp/test | grep 'succeeded in sending to' > /tmp/test_status; ls -l /tmp/test_status | awk -F ' ' '{print $5}'`

# Mail sent only to 'Shreeshail'
if [ $status_flag -eq 0 ]; then cat /tmp/test | mail -s "[WARNING] Site Analyzer CRON - FAILING !!! : $(date +'%d %b %Y %r')" sgumgeri@akamai.com -a "From: sgumgeri@akamai.com"; else cat /tmp/test |mail -s "Site Analyzer CRON Status : $(date +'%d %b %Y %r')" sgumgeri@akamai.com -a "From: automon@akamai.com";fi

