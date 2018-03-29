filename=allgdns_15days_result_delta25_`date +%F`
nohup python /home/sgumgeri/ADB/DATA_GAP/data_gap.py -g ma.*.* -d 15 -bs -p 15 -n 100 > /home/sgumgeri/ADB/DATA_GAP/results_data_gap/$filename &
