#!/usr/bin/env python

"""
Author - Shreeshail G
Date Created - 06/03/2016
Last Modified - 03/03/2017
This function is used to generate the HTML msg body
"""
import sys 
import shutil
import glob
import time
import socket
import os.path
import threading
import subprocess
from datetime import datetime, timedelta
from time import gmtime, strftime
# MAIL
import smtplib
from email.MIMEText import MIMEText

query_ip = socket.gethostbyname('dna.dev.query.akadns.net')
query_io_global = ''
query_DP_Down_machine_check = ''' 
 SELECT dm.ip ip_key,
         LABEL,
         DATACENTER,
         REPLICA,
         STATUS,
         ROLE,
         NWTAG
    FROM dna_machines dm,
         (
           SELECT machineip,
                  physregion region
             FROM machinedetails
            WHERE machineip NOT IN (
                    SELECT machineip
                      FROM installinfo
                  )
                  AND physregion IN (
                    SELECT physregion
                      FROM machinedetails M,
                           installinfo I
                     WHERE M.machineip = I.machineip
                  )
         ) it
   WHERE it.machineip = dm.ip
and nwtag like '%DATAPROCESSOR%' 
and status not like 'INACTIVE'
;
   \n'''
def execute_query_DP_Down_machine_check():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    sock.send(query_DP_Down_machine_check)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    return query_io

query_DP_Down_machine_check_2 = '''
select s.machineip, s.action,  s.service, d.label, d.replica, d.role, d.status, d.nwtag, s.reason  
from stickyreasons s, dna_machines d 
where d.ip =machineip  
and d.NWTAG = "DATAPROCESSOR::DOWNLOADER" 
and s.reason not like "%DPRoleSetup%" 
group by s.machineip;
\n'''

def execute_query_DP_Down_machine_check_2():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))    
    sock.send(query_DP_Down_machine_check_2)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    return query_io

query_DP_monitor = '''
SELECT machine_ip ip_key,
          directory_name,
         nwtag, role,
         label,
         num_files,
         total_size / (1000 * 1000 ) size_in_Mb,
         replica,
         (latest_timestamp - oldest_timestamp)/(3600) as Hours,
         tonum(oldest_timestamp) oldest_timestamped_file,
         tonum(latest_timestamp) latest_timestamped_file

    FROM input_mon_stats ims,
         dna_machines dm
   WHERE dm.ip = ims.machine_ip
         AND  (total_size / (1000 * 1000  )  >  500 or  num_files > 250)
         AND directory_name not like '%/dpdone/%'
         AND (latest_timestamp - oldest_timestamp)/(3600) > 4
         AND directory_name not like '/usr/local/akamai/logs/fblb/'
         AND NOT directory_name SIMILAR '^.*work\/$'
         AND NOT directory_name SIMILAR '^.*index\/$'
         AND status = 'ACTIVE'
         AND nwtag = 'DATAPROCESSOR::DOWNLOADER'
GROUP by 1,2
order by oldest_timestamped_file asc;
   \n'''

#This Function Takes the query and gives the result of it
def execute_DP_monitor():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    sock.send(query_DP_monitor)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    return query_io

query_outgoing_dir = '''
SELECT replica, sum(num_files)
    FROM input_mon_stats ims, 
         dna_machines dm
   WHERE dm.ip = ims.machine_ip
         and directory_name like '%/downloader/outgoing/%' 
         group by replica;
\n'''


def execute_outgoing_dir():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    sock.send(query_outgoing_dir)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    return query_io

query_1day_stats = '''
select replica, sum(num_input_files) as in_files, sum(num_output_files) as out_files 
from fblb_stats_1day fb, dna_machines dm 
where fb.ip = dm.ip
           group by replica;
\n'''

def execute_1day_stats():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    # print "in_files   --> input to /downloader/outgoing/"
    # print "out_files  --> input to /fblb_pull/host/"
    sock.send(query_1day_stats)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    return query_io

query_FBLB_RTProc_Backlog = '''
SELECT  dm.ip, directory, size, dm.label, dm.replica
FROM fblbDirInfo fblb, 
    dna_machines dm
WHERE dm.ip = fblb.ip
     AND directory like '/ghostcache/fblb_pull/toscp/%'  
     AND size > 50000;
\n'''


def execute_FBLB_RTProc_Backlog():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    sock.send(query_FBLB_RTProc_Backlog)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    return query_io

query_VD_Down_machine_check = '''
select * from (
SELECT d.ip ip,
d.datacenter,
d.label label,
s.reason
FROM stickyreasons s,
dna_machines d
WHERE d.ip = s.machineip
AND d.role IN ('Storage')
AND (s.action = 'suspend' OR s.action = 'stop')
AND s.service SIMILAR '^adb.*$'
AND label like "%DADB_VD%"
and (s.reason LIKE '%OIKed%' or s.reason like "%-%")
AND d.label not like "%SPARE%" group by 1
union all 
SELECT ip,
datacenter,
LABEL,
"down/NIE"
FROM dna_machines
WHERE ip NOT IN (
SELECT ip
FROM system
)
AND label like "%DADB_VD%"
AND NOT LABEL SIMILAR '^..SPARE.$'
ORDER BY 2 
) a group by 1
order by 3 ;
'''

def execute_query_VD_Down_machine_check():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    sock.send(query_VD_Down_machine_check)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    return query_io


def send_mail():
    current_time = int(time.time())
    current_time = gmtime(current_time)
    human_readable_string = strftime("%d %b %Y", current_time)

    TEXT = "<html><head></head><body>"
    TEXT = TEXT + "<p>NOTE : This Mail is sent to below Mailing list : sgumgeri@akamai.com; bfakrudd@akamai.com; lbabu@akamai.com; hshekhar@akamai.com; sreddy@akamai.com; shkundap@akamai.com; sanrao@akamai.com; rchoudhu@akamai.com ;gyadav@akamai.com;sasati@akamai.com</p>"
    TEXT = TEXT + "<p>Hi ALL</p>"
    
    
    lines_of_file =  execute_query_DP_Down_machine_check()
    # print lines_of_file     
    TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"   
    for each_line in lines_of_file.split('\n'):
        if 'rows selected' not in each_line and '---' not in each_line:
            TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
            #TEXT = TEXT + "<td style='border-right: 1px dashed black; border-collapse: collapse;'> Time </td>\n"
            for each_column in each_line.split():
                
                TEXT = TEXT + "<td align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_column + "</td>\n"
        TEXT = TEXT + "</tr>\n"
    TEXT = TEXT + "<p><br />------------------------ Monitoring FBLB & DP DOWN Machines - I ------------------------</p>\nQuery Link : https://www.nocc.akamai.com/miniurl/?id=abf4</p>"


    
    lines_of_file =  execute_query_DP_Down_machine_check_2()
    # print lines_of_file     
    TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"   
    for each_line in lines_of_file.split('\n'):
        if 'rows selected' not in each_line and '---' not in each_line:
            TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
            #TEXT = TEXT + "<td style='border-right: 1px dashed black; border-collapse: collapse;'> Time </td>\n"
            for each_column in each_line.split():
                
                TEXT = TEXT + "<td align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_column + "</td>\n"
        TEXT = TEXT + "</tr>\n"    
    TEXT = TEXT + "<p><br />------------------------ Monitoring FBLB & DP DOWN Machines - II ------------------------</p>\nQuery Link : https://www.nocc.akamai.com/miniurl/?id=b695 </p>"

    
    lines_of_file =  execute_DP_monitor()
    # print lines_of_file    
    TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"   
    row = 1
    column = 0
    for each_line in lines_of_file.split('\n'):
        if 'rows selected' not in each_line and '---' not in each_line and len(each_line.split()) > 0:
            TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
            color = "WHITE"
            if row != 1 :
                ip,directory,nwtag,role,label,num_size,size_in_Mb,replica,Hours,oldest_timestamp,latest_timestamp = each_line.split()
                Hours = Hours.replace(',', '')
                if int(Hours) > 10 and int(Hours) <20:
                    color = "#FF9900"                
                elif  int(Hours) >=20 :
                    color = "RED"

            for each_column in each_line.split():
                
                column = column + 1
                if row != 1 :
                    if column == 11 or column == 10 :
                        number = ''.join([i for i in each_column if i.isdigit()])
                        timestamp = int (number)
                        timestamp = gmtime(timestamp)
                        each_column = strftime("%d-%b-%Y %I:%M:%S %p", timestamp)
                    # if abs(int(each_line.split()[8])) > 10 and column == 9:
                    #     color = "#FF9900"    
                TEXT = TEXT + "<td bgcolor="+color+" align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_column + "</td>\n"
        TEXT = TEXT + "</tr>\n"
        row = row + 1
        column = 0
    TEXT = TEXT + "<p><br />------------------------ Monitoring FBLB & DP Machines ---------------------------------</p>\nQuery Link : https://www.nocc.akamai.com/miniurl/?id=a9ec</p>"

#########################################
    lines_of_file =  execute_query_VD_Down_machine_check()
    # print lines_of_file    
    TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"   
    for each_line in lines_of_file.split('\n'):
        if 'rows selected' not in each_line and '---' not in each_line and len(each_line.split()) > 0:
            color = "WHITE"
            TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
            #TEXT = TEXT + "<td style='border-right: 1px dashed black; border-collapse: collapse;'> Time </td>\n"
            for each_column in each_line.split():
                
                TEXT = TEXT + "<td bgcolor="+color+" align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_column + "</td>\n"
        TEXT = TEXT + "</tr>\n"
        row = row + 1

    TEXT = TEXT + "<p><br />----------- Monitoring VD Down Machine ----------------------</p>\nQuery Link : https://www.nocc.akamai.com/miniurl/?id=fa2f</p>"

###############################################
    lines_of_file =  execute_FBLB_RTProc_Backlog()
    # print lines_of_file     
    TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"   
    for each_line in lines_of_file.split('\n'):
        if 'rows selected' not in each_line and '---' not in each_line and len(each_line.split()) > 0:
            color = "WHITE"
            TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
            #TEXT = TEXT + "<td style='border-right: 1px dashed black; border-collapse: collapse;'> Time </td>\n"
            for each_column in each_line.split():
                
                TEXT = TEXT + "<td bgcolor="+color+" align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_column + "</td>\n"
        TEXT = TEXT + "</tr>\n"
        row = row + 1

    TEXT = TEXT + "<p><br />----------- Monitoring FBLB-RTProc Backlog '*/fblb_pull/toscp/*' ----------------------</p>\nQuery Link : https://www.nocc.akamai.com/miniurl/?id=a9fe</p>"

    lines_of_file =  execute_outgoing_dir()
    # print lines_of_file     
    TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"   
    for each_line in lines_of_file.split('\n'):
        if 'rows selected' not in each_line and '---' not in each_line:
            TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
            #TEXT = TEXT + "<td style='border-right: 1px dashed black; border-collapse: collapse;'> Time </td>\n"
            for each_column in each_line.split():
                
                TEXT = TEXT + "<td align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_column + "</td>\n"
        TEXT = TEXT + "</tr>\n"
    TEXT = TEXT + "<p><br />--------------------- Monitoring '/downloader/outgoing' directory -----------------------</p>\nQuery Link : https://www.nocc.akamai.com/miniurl/?id=9fc3</p>"

    lines_of_file =  execute_1day_stats()
    # print lines_of_file     
    TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"   
    for each_line in lines_of_file.split('\n'):
        if 'rows selected' not in each_line and '---' not in each_line:
            TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
            #TEXT = TEXT + "<td style='border-right: 1px dashed black; border-collapse: collapse;'> Time </td>\n"
            for each_column in each_line.split():
                
                TEXT = TEXT + "<td align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_column + "</td>\n"
        TEXT = TEXT + "</tr>\n"
    TEXT = TEXT + "<p><br />-------------------------Previous Day stats -------------------------------------------</p>\nQuery Link : https://www.nocc.akamai.com/miniurl/?id=9fc4</p>"



    TEXT = TEXT + "</table>\n<br /><p> Regards <br/>DNA System Operation Team</p>\n</div>\n</body>\n</html>"  

    print TEXT


    msg = MIMEText(TEXT,'html')
    msg['Subject'] = "1. Monitoring DP Machines : "+ human_readable_string
    # Send the mail
    FROM = 'automon-media@akamai.com'
    #TO = ['sgumgeri@akamai.com','bfakrudd@akamai.com'] 
    TO = ['sasati@akamai.com','sgumgeri@akamai.com','gyadav@akamai.com', 'bfakrudd@akamai.com', 'sreddy@akamai.com', 'shkundap@akamai.com','lbabu@akamai.com', 'hshekhar@akamai.com', 'sanrao@akamai.com', 'rchoudhu@akamai.com']
    mailer = smtplib.SMTP('')
    mailer.connect()
    mailer.sendmail(FROM, TO, msg.as_string())
    mailer.close()

send_mail()
