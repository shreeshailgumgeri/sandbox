/usr/bin/python ~/OPS/email_cron_dir/monitoring_index_files_morning.py 2>&1 > /tmp/log ; /usr/bin/python ~/OPS/email_cron_dir/monitoring_index_files_compare_num.py | mail -s "3. Monitoring Index file : $(date +'%d %b %Y')" sgumgeri@akamai.com,gyadav@akamai.com,hshekhar@akamai.com,bfakrudd@akamai.com,jadas@akamai.com,sreddy@akamai.com,shkundap@akamai.com,sanrao@akamai.com,sausingh@akamai.com,rchoudhu@akamai.com,sasati@akamai.com  -a "From: automon-media@akamai.com"

/usr/bin/python ~/OPS/email_cron_dir/monitoring_index_files_morning.py 2>&1 > /tmp/log ; /usr/bin/python ~/OPS/email_cron_dir/monitoring_index_files_compare_num_above_20P.py | mail -s "4. Monitoring Index file(More than 20 percent diff) : $(date +'%d %b %Y')" sgumgeri@akamai.com,gyadav@akamai.com,hshekhar@akamai.com,bfakrudd@akamai.com,jadas@akamai.com,sreddy@akamai.com,shkundap@akamai.com,sanrao@akamai.com,sausingh@akamai.com,rchoudhu@akamai.com,sasati@akamai.com  -a "From: automon-media@akamai.com"
