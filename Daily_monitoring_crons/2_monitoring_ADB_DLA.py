#!/usr/bin/env python

"""
Author - Shreeshail G
Date - 06/03/2016

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
query_ADB = '''
  SELECT  a.ip, 
         a.dir_name, 
         d.label, 
         a.num_files, 
         a.total_size / (1024 * 1024) backlog_size_GB,
         d.REPLICA        
    FROM adb_input_mon a, 
         dna_machines d
   WHERE a.dir_name NOT IN ('/ghostcache/analyticsdb/backup/error')
         AND (a.total_size / (1024 * 1024)) >= 20
         AND a.ip = d.ip
         AND label not like '%SPARE%'
         AND status not like 'INACTIVE'
and d.ip not in(
select ip from (
SELECT  d.ip ip,
         d.datacenter,
         d.label label,
         s.reason
    FROM stickyreasons s,
         dna_machines d
   WHERE d.ip = s.machineip
         AND d.role IN ('Storage')
         AND (s.action = 'suspend' OR s.action = 'stop')
         AND s.service SIMILAR '^adb.*$'
         AND label like "%DADB%"
         and ( s.reason like "%-%")
         AND d.label not like "%SPARE%" group by 1
) a group by 1
)
   ORDER BY backlog_size_GB DESC;

\n'''

def ADB_overall_query():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    sock.send(query_ADB)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    return query_io
    

query_control_server = '''
 SELECT  a.ip, 
         a.dir_name, 
         d.label, 
         a.num_files, 
         a.total_size / (1024 * 1024 ) backlog_size_GB,
         d.REPLICA        
    FROM adb_input_mon a, 
         dna_machines d
   WHERE a.dir_name NOT IN ('/ghostcache/analyticsdb/backup/error')
         AND d.dnsname in ('adbcs1.east.dna.akamai.com','adbcs2.east.dna.akamai.com', 'adbcs1.central.dna.akamai.com', 'adbcs2.central.dna.akamai.com')
         AND a.ip = d.ip
         AND a.num_files > 0
   ORDER BY 5 DESC;  
\n'''

def ADB_query_control_server():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    sock.send(query_control_server)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    return query_io
    


#Query central
query_dla = '''
SELECT d.ip,d.label,a.dir_name,a.num_files,a.total_size/ (1024 * 1024) backlog_size_GB,a.max_size/(1024 * 1024) max_size_GB,d.replica
 FROM adb_input_mon a,
               dna_machines d 
where d.label like 'DADB_MA_DLA_%' and 
a.dir_name Not IN ('/ghostcache/analyticsdb/backup/error')   AND  (a.total_size / (1024 * 1024)) >= 20 and d.ip=a.ip ORDER BY 5 DESC;
\n'''

# #Query EAST
# query_East = '''
# SELECT d.ip, tonum(time) time, dir_name, num_files, total_size, max_size, replica
#     FROM adb_input_mon a,
#                dna_machines d 
# where d.dnsname in ('adb304.east.dna.akamai.com','adb354.east.dna.akamai.com')
# group by 1,3
# order by 4 desc;
# \n'''

def execute_dla_query():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    sock.send(query_dla)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    return query_io
    

# def execute_query_east():
#     sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
#     sock.connect((query_ip, 13000))
#     sock.send(query_East)
#     query_io = ''

#     while True:
#         data = sock.recv(1024)
#         query_io = '%s%s' % (query_io, data)

#         if data == '' or 'rows selected' in data:
#             break

#     return query_io
    

def send_mail():
    current_time = int(time.time())
    current_time = gmtime(current_time)
    human_readable_string = strftime("%d %b %Y", current_time)

    TEXT = "<html><head></head><body>"
    TEXT = TEXT + "<p>NOTE : This Mail is sent to below Mailing list : sgumgeri@akamai.com;gyadav@akamai.com; bfakrudd@akamai.com; ramyan@akamai.com; rmorarka@akamai.com; sanrao@akamai.com; analyticsdb@akamai.com; hshekhar@akamai.com; lbabu@akamai.com</p>"
    TEXT = TEXT + "<p>Hi ALL</p>"
    
    
    lines_of_file =  ADB_overall_query()
    # print lines_of_file     
    TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"   
    row = 1
    column = 0
    for each_line in lines_of_file.split('\n'):
        if 'rows selected' not in each_line and '---' not in each_line and len(each_line.split()) > 0:
            TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
            color = "WHITE"
            if row != 1 :
                backlog_size_GB =  each_line.split()[4].replace(",", "")
                if int(backlog_size_GB) > 40 and int(backlog_size_GB) <80:
                    color = "#FF9900"                
                elif  int(backlog_size_GB) >=80 :
                    color = "RED"

            for each_column in each_line.split():
                
                column = column + 1
                TEXT = TEXT + "<td bgcolor="+color+" align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_column + "</td>\n"
        TEXT = TEXT + "</tr>\n"
        row = row + 1
        column = 0
    TEXT = TEXT + "<p><br />--------- ADB Over all Monitoring Sorted By backlog_size_GB ----------------------------</p>\nQuery Link : https://www.nocc.akamai.com/miniurl/?id=b730</p>"


    
    lines_of_file =  ADB_query_control_server()
    # print lines_of_file     
    TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"   
    for each_line in lines_of_file.split('\n'):
        if 'rows selected' not in each_line and '---' not in each_line:
            TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
            for each_column in each_line.split():
                TEXT = TEXT + "<td align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_column + "</td>\n"
        TEXT = TEXT + "</tr>\n"    
    TEXT = TEXT + "<p><br />--------- Control Server Sorted By backlog_size_GB----------------------------</p>\nQuery Link : https://www.nocc.akamai.com/miniurl/?id=a8f1 </p>"

    row = 1
    column = 0    
    lines_of_file =  execute_dla_query()
    # print lines_of_file    
    TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"   
    for each_line in lines_of_file.split('\n'):
        if 'rows selected' not in each_line and '---' not in each_line:
            TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
            for each_column in each_line.split():
                column = column + 1
                if row != 1 :
                    if column == 2 :
                        number = ''.join([i for i in each_column if i.isdigit()])
			if number.isdigit():
                        	timestamp = int (number)
                        	timestamp = gmtime(timestamp)
                        	each_column = strftime("%d-%b-%Y %I:%M:%S %p", timestamp)                
                TEXT = TEXT + "<td align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_column + "</td>\n"
        TEXT = TEXT + "</tr>\n"   
        row = row + 1
        column = 0         
    TEXT = TEXT + "<p><br />------------------------- DLA Machines Backlog -------------------------------------------- </p>"

    # row = 1
    # column = 0   
    # lines_of_file =  execute_query_east()
    # # print lines_of_file     
    # TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"   
    # for each_line in lines_of_file.split('\n'):
    #     if 'rows selected' not in each_line and '---' not in each_line and len(each_line.split()) > 0:
    #         color = "WHITE"
    #         TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
    #         #TEXT = TEXT + "<td style='border-right: 1px dashed black; border-collapse: collapse;'> Time </td>\n"
    #         for each_column in each_line.split():
    #             column = column + 1
    #             if row != 1 :
    #                 if column == 2 :
    #                     number = ''.join([i for i in each_column if i.isdigit()])
    #                     if number.isdigit():
    #                             timestamp = int (number)
    #                             timestamp = gmtime(timestamp)
    #                             each_column = strftime("%d-%b-%Y %I:%M:%S %p", timestamp)  
    #             TEXT = TEXT + "<td bgcolor="+color+" align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_column + "</td>\n"
    #     TEXT = TEXT + "</tr>\n"
    #     row = row + 1
    #     column = 0  
    # TEXT = TEXT + "<p><br />-------------------------DLA Machines EAST--------------------------------------------</p>\nQuery Link : https://www.nocc.akamai.com/miniurl/?id=a8f0</p>"


    TEXT = TEXT + "</table>\n<br /><p> Regards <br/>DNA System Operation Team</p>\n</div>\n</body>\n</html>"  

    print TEXT


    msg = MIMEText(TEXT,'html')
    msg['Subject'] = "2. Monitoring BACKLOG on ADB & DLA Machines : "+ human_readable_string
    # Send the mail
    FROM = 'automon-media@akamai.com'
    #TO = ['sgumgeri@akamai.com'] #['ramyan@akamai.com', 'pajoseph@akamai.com']
    TO = ['sgumgeri@akamai.com','gyadav@akamai.com', 'bfakrudd@akamai.com', 'hshekhar@akamai.com', 'agoyal@akamai.com', 'ramyan@akamai.com', 'rmorarka@akamai.com', 'vsanaka@akamai.com', 'sanrao@akamai.com', 'analyticsdb@akamai.com', 'lbabu@akamai.com']
    mailer = smtplib.SMTP('')
    mailer.connect()
    mailer.sendmail(FROM, TO, msg.as_string())
    mailer.close()

send_mail()
