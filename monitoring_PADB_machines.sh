mkdir -p ~/PADB
log=$(date +'%d%b%Y_%T')
python /home/sgumgeri/7_monitoring_PADB_hung_v4.py >> ~/PADB/PADB_logs_$log

