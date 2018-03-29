#!/usr/bin/env python

"""
Author - Shreeshail G
Date - 29/09/2016
This Script is to Check the Telnet connection status of the PADB machine
"""
import time
from datetime import datetime, timedelta
from time import gmtime, strftime
import socket,telnetlib

import smtplib
from email.MIMEText import MIMEText

PADB_BOXes = '''184.50.224.8
72.247.34.95
72.247.34.96
72.247.34.97
72.247.34.69
184.50.224.51
184.50.224.50
72.247.34.75
72.247.34.74
184.50.224.5
184.50.224.4
184.50.224.6'''

"""
Portal Pointing to PADB Machines can be found by
Link : https://portalops.akamai.com/tools/sqltool/CREP
Query : select distinct host from CKQD_SERVER_RANK where HEALTH =1;
OUTPUT
HOST
72.247.34.96
72.247.34.69
72.247.34.95
184.50.224.50
"""

PORTAL_POINTING_PADB_BOXes = '''
72.247.34.96
72.247.34.69
72.247.34.95
184.50.224.50'''

"""
Range :
Query To get the Range its pointing
Agregator : ddc.dev.query.akadns.net
Query : 
select * from perfanalyticsdb_ckqd_testrange order by 1,2;
"""

list_good_machines = []
list_hung_machines = []

query_ip = socket.gethostbyname('ddc.dev.query.akadns.net')
query_range_check = ''' 
select * from perfanalyticsdb_ckqd_testrange order by 1,2;
;
   \n'''

def execute_query_range_check():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    sock.send(query_range_check)
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

    TEXT = "<html>\n<head></head>\n<body>\n"
    TEXT = TEXT + "<p>NOTE : This Mail is sent to below Mailing list :  utank@akamai.com; ajathann@akamai.com; lbabu@akamai.com ;  vsrivast@akamai.com;asahu@akamai.com; mcs-blr@akamai.com; hshekhar@akamai.com</p>"
    TEXT = TEXT + "<p>Hi ALL</p><br>This mail is about Checking the Connnection STATUS of PADB Machines<br><br>"
    dict_ip_error = {}
    for ip in PADB_BOXes.split('\n') :
      print "-----"+ip+"------"
      sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
      try :
        sock.connect((ip, 8458))
        sock.settimeout(10)
        sock.send("Hi" + "\n")
        from_sock = sock.recv(1024)
        print "ACTIVE"
        dict_ip_error[ip]="ACTIVE"
        list_good_machines.append(ip)

      except Exception as e:
          error_message = str(e)
          if 'timed out' in error_message :
            print 'MACHINE DOWN :: ==> ' + error_message
            dict_ip_error[ip]="MACHINE DOWN"
            list_hung_machines.append(ip)
          elif 'Connection refused' in error_message :
            print 'CONNECTION ISSUE :: ==> ' + error_message
            dict_ip_error[ip]="CONNECTION HUNG "
            list_hung_machines.append(ip)
          else :
              print "Error on connect: %s" % e
              dict_ip_error[ip]= error_message
              list_hung_machines.append(ip)

      sock.close()

    TEXT = TEXT + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"
    TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
    TEXT = TEXT + "<td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'> <span style=font-weight:bold> PADB_BOXes </span></td> <td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'> <span style=font-weight:bold>Connection Status </span></td><td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'> <span style=font-weight:bold>Is PORTAL Pointing</td><td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'> <span style=font-weight:bold>Range</td>\n"


# Below Block of code is to get the range of the IP
# START
    lines_of_file =  execute_query_range_check()
    dict_ip = {}
    ip = ''
    row = 1
    for line in lines_of_file.split('\n') :
      if row == 1 or row == 2 :
          # print 'LINE = '+ str(row)
          row = row +1 
          pass
      elif line == '' or 'rows' in line:
        break
      else :
        # print 'IN ELSE case'
        ip, r1, r2 = line.split()
        # print ip, r1, r2
        if ip in dict_ip : 
          temp = dict_ip[ip]
          dict_ip[ip] =temp + '[ ' + r1+'-'+r2+ ' ]'
        else :
          dict_ip[ip] ='[ ' + r1+'-'+r2+ ' ]'
    Range = 'NULL'
# END

    for ip in list_good_machines :
      STATUS = dict_ip_error[ip]
      # Status = str(dict_ip_error[ip])
      if ip in PORTAL_POINTING_PADB_BOXes.split() :
          Is_PORTAL_pointing = 'YES'
          color2="GREEN"
      else :
          Is_PORTAL_pointing = 'NO'
          color2="RED"
      if ip in dict_ip :
        Range = dict_ip[ip]

      color = "GREEN"
      TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
      TEXT = TEXT + "<td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'> <font color="+color+">" + ip + "</td><td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'><font color="+color+"> "+STATUS+"  </td><td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'><font color="+color2+">"+Is_PORTAL_pointing+"</td><td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'> "+Range+"</td>\n"
      Range = 'NULL'

    TEXT = TEXT + "</tr>\n"  
    

    for ip in list_hung_machines :
      STATUS = dict_ip_error[ip]
      if ip in PORTAL_POINTING_PADB_BOXes.split() :
          Is_PORTAL_pointing = 'YES'
          color2="GREEN"
      else :
          Is_PORTAL_pointing = 'NO'
          color2="RED"

      if ip in dict_ip :
        Range = dict_ip[ip]

      color = "RED"
      TEXT = TEXT + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
      TEXT = TEXT + "<td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'> <font color="+color+">" + ip + "</td><td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'><font color="+color+"> "+STATUS+" </td><td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'><font color="+color2+">"+Is_PORTAL_pointing+"</td><td align='center' style='border-right: 1px dashed black; border-collapse: collapse;'> "+Range+"</td>\n"
      Range = 'NULL'

    TEXT = TEXT + "</tr>\n"  

    TEXT = TEXT + "</table>\n<br /><p> Regards <br/>MCS Team</p>\n</div>\n</body>\n</html>"  
    print '------------------------- HTML SCRIPT ------------------------------------------------'
    print TEXT
    print '--------------------------------------------------------------------------------------'

    msg = MIMEText(TEXT,'html')
    msg['Subject'] = "7. Monitoring PADB Machines : "+ human_readable_string
    # Send the mail
    FROM = 'automon-media@akamai.com'
    # TO = ['sgumgeri@akamai.com'] 
    TO = ['hshekhar@akamai.com','sgumgeri@akamai.com','utank@akamai.com', 'ajathann@akamai.com', 'lbabu@akamai.com','asahu@akamai.com', 'vsrivast@akamai.com' ,'mcs-blr@akamai.com']
    mailer = smtplib.SMTP('')
    mailer.connect()
    mailer.sendmail(FROM, TO, msg.as_string())
    mailer.close()

    print '\n--------------------------------------------------------------------------------------\n\n'

send_mail()

