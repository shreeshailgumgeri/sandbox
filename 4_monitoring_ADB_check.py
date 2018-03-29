#!/usr/bin/env python
'''
This scripts is for Monitoring DP Machines (under DNA Aggregator)
It populates 4 queries and sends it through mail.
Author : Shreeshail G
'''

import socket
import time

print "======================================================================================="
print "        WELCOME, Today's Date is : " + time.strftime("%a, %d %b %Y %H:%M:%S") 
print "=======================================================================================\n\n"

#Get the aggregator ip
query_ip = socket.gethostbyname('dna.dev.query.akadns.net')

#Query 1
query_QOS_down = '''
select * from (
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
         AND label like "%DADB_QOS%"
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
         AND label like "%DADB_QOS%"
         AND NOT LABEL SIMILAR '^.*.SPARE.*$'
   ORDER BY 2 
) a group by 1
order by 3 
;
\n'''

#This Function Takes the query and gives the result of it
def execute_query_QOS_down():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    print "_______________________________________________________________________________________"
    print "************************ OVERALL DOWN Machines  ***************************************"
    print 'Query Link : https://www.nocc.akamai.com/miniurl/?id=b183'
    print "_______________________________________________________________________________________"
    print "_______________________________________________________________________________________"
    print "------------------------ QOS DOWN Machines  ---------------------------------"
    print 'Query Link : https://www.nocc.akamai.com/miniurl/?id=a8f3'
    print "_______________________________________________________________________________________"
    
    sock.send(query_QOS_down)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    print query_io

execute_query_QOS_down()

query_BOSS_down = ''' 
select * from (
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
         AND label like "%DADB_BOSS%"
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
         AND label like "%DADB_BOSS%"
         AND NOT LABEL SIMILAR '^.*.SPARE.*$'
   ORDER BY 2 
) a group by 1
order by 3 
;
\n'''

#This Function Takes the query and gives the result of it
def execute_query_BOSS_down():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    print "_______________________________________________________________________________________"
    print "------------------------ BOSS DOWN Machines  ---------------------------------"
    print 'Query Link : https://www.nocc.akamai.com/miniurl/?id=c265'
    print "_______________________________________________________________________________________"
    
    sock.send(query_BOSS_down)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break

    print query_io

execute_query_BOSS_down()

#Query2

query_MA_down = '''
select * from (
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
         AND label like "%DADB_MA%"
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
         AND label like "%DADB_MA%"
         AND NOT LABEL SIMILAR '^.*.SPARE.*$'
   ORDER BY 2 
) a group by 1
order by 3 
;
\n'''

#This Function Takes the query and gives the result of it
def execute_query_MA_down():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    print "_______________________________________________________________________________________"
    print "------------------------ MA DOWN Machines  ---------------------------------"
    print 'Query Link : https://www.nocc.akamai.com/miniurl/?id=a8f4'
    print "_______________________________________________________________________________________"
    
    sock.send(query_MA_down)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    print query_io

execute_query_MA_down()


query_NM_down = '''
select * from (
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
         AND label like "%DADB_NM%"
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
         AND label like "%DADB_NM%"
         AND NOT LABEL SIMILAR '^.*.SPARE.*$'
   ORDER BY 2 
) a group by 1
order by 3 
;
\n'''

#This Function Takes the query and gives the result of it
def execute_query_NM_down():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    print "_______________________________________________________________________________________"
    print "------------------------ NM DOWN Machines  ---------------------------------"
    print 'Query Link : https://www.nocc.akamai.com/miniurl/?id=a8f5'
    print "_______________________________________________________________________________________"
    
    sock.send(query_NM_down)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break
    
    print query_io

execute_query_NM_down()

query_QN_CS_node_down = ''' 
select * from (
SELECT  d.ip ip,
         d.datacenter,
         d.label label,
         d.status,
         d.product,
        d.nwtag,
        d.role,
         s.reason
    FROM stickyreasons s,
         dna_machines d
   WHERE d.ip = s.machineip
         AND d.role IN ('Storage')
         AND (s.action = 'suspend' OR s.action = 'stop')
         AND s.service SIMILAR '^adb.*$'
         AND  label not like 'DADB%' and label not like 'DUMMY' and d.status like 'ACTIVE' and nwtag like 'ANALYTICSDB%' and (d.product in( 'QUERYNODE','CS') and s.reason not like 'OIKed')
         and (s.reason LIKE '%OIKed%' or s.reason like "%-%")
         AND d.label not like "%SPARE%" group by 1
union all 
  SELECT ip,
         datacenter,
         LABEL,
         status,
         product,
        nwtag,
        role,
         "down/NIE"

   FROM dna_machines
   WHERE ip NOT IN (
           SELECT ip
             FROM system
         )
         AND  label not like 'DADB%' and label not like 'DUMMY' and status like 'ACTIVE'  and nwtag like 'ANALYTICSDB%' 
         AND NOT LABEL SIMILAR '^.*.SPARE.*$'
   ORDER BY 2 
) a group by 1
order by 3 
;

\n'''

#This Function Takes the query and gives the result of it
def execute_query_QN_CS_node_down():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    print "_______________________________________________________________________________________"
    print "------------------------ QN_CS_node DOWN Machines  ---------------------------------"
    print 'Query Link : https://www.nocc.akamai.com/miniurl/?id=bcc8'
    print "_______________________________________________________________________________________"

    sock.send(query_QN_CS_node_down)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break

    print query_io

execute_query_QN_CS_node_down()

query_non_datanode_down = '''
select * from (
SELECT  d.ip ip,
         d.datacenter,
         d.label label,
         d.status,
         d.product,
        d.nwtag,
        d.role,
         s.reason
    FROM stickyreasons s,
         dna_machines d
   WHERE d.ip = s.machineip
         AND d.role IN ('Storage')
         AND (s.action = 'suspend' OR s.action = 'stop')
         AND s.service SIMILAR '^adb.*$'
         AND  label not like 'DADB%' and label not like 'ADBQ%' and label not like 'DUMMY' and d.status like 'ACTIVE' and nwtag like 'ANALYTICSDB%' and d.product not in ('QUERYNODE', 'CS')
         and (s.reason LIKE '%OIKed%' or s.reason like "%-%")
         AND d.label not like "%SPARE%" group by 1
union all 
  SELECT ip,
         datacenter,
         LABEL,
         status,
         product,
        nwtag,
        role,
         "down/NIE"

   FROM dna_machines
   WHERE ip NOT IN (
           SELECT ip
             FROM system
         )
         AND  label not like 'DADB%' and label not like 'DUMMY' and status like 'ACTIVE'  and nwtag like 'ANALYTICSDB%' and product not like 'QUERYNODE'
         AND NOT LABEL SIMILAR '^.*.SPARE.*$'
   ORDER BY 2 
) a group by 1
order by 3 

;
\n'''

#This Function Takes the query and gives the result of it
def execute_query_non_datanode_down():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    print "_______________________________________________________________________________________"
    print "------------------------ non_datanode DOWN Machines  ---------------------------------"
    print 'Query Link : https://www.nocc.akamai.com/miniurl/?id=bcc8'
    print "_______________________________________________________________________________________"

    sock.send(query_non_datanode_down)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break

    print query_io

execute_query_non_datanode_down()

query_PA_down = '''
select * from (
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
         AND label like "%DADB_PA%"
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
         AND label like "%DADB_PA%"
         AND NOT LABEL SIMILAR '^.*.SPARE.*$'
   ORDER BY 2 
) a group by 1
order by 3 
;
\n'''

#This Function Takes the query and gives the result of it
def execute_query_PA_down():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    print "_______________________________________________________________________________________"
    print "------------------------ PA DOWN Machines  ---------------------------------"
    print 'Query Link : https://www.nocc.akamai.com/miniurl/?id=b182'
    print "_______________________________________________________________________________________"

    sock.send(query_PA_down)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break

    print query_io

execute_query_PA_down()

query_cassandra_down = '''
select * from (
SELECT  d.ip ip,
         d.datacenter,
         d.label label,
         s.reason
    FROM stickyreasons s,
         dna_machines d
   WHERE d.ip = s.machineip
         AND d.role IN ('Storage')
         AND d.status IN ('ACTIVE')
         AND (s.action = 'suspend' OR s.action = 'stop')
         AND s.service SIMILAR '^adb.*$'
         AND label like "%CASSANDRA%"
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
         AND label like "%CASSANDRA%"
         AND NOT LABEL SIMILAR '^.*.SPARE.*$'
   ORDER BY 2 
) a group by 1
order by 3 
;
\n'''

#This Function Takes the query and gives the result of it
def execute_query_cassandra_down():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    print "_______________________________________________________________________________________"
    print "------------------------ CASSANDRA DOWN Machines  ---------------------------------"
    print "_______________________________________________________________________________________"

    sock.send(query_cassandra_down)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break

    print query_io

execute_query_cassandra_down()


query_beacon_server_down = '''
select * from (
SELECT  d.ip ip,
         d.datacenter,
         d.label label,
         s.reason
    FROM stickyreasons s,
         dna_machines d
   WHERE d.ip = s.machineip
         AND d.role IN ('QOSPROC')
         AND d.status IN ('ACTIVE')
         AND (s.action = 'suspend' OR s.action = 'stop')
         AND s.service SIMILAR '^adb.*$'
         AND NWTAG like "BEACON%"
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
         AND NWTAG like "BEACON%"
         AND NOT LABEL like '%SPARE%'
         AND NOT LABEL like '%DUMMY%'
   ORDER BY 2 
) a group by 1
order by 3 
;
\n'''

#This Function Takes the query and gives the result of it
def execute_query_beacon_server_down():
    sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    sock.connect((query_ip, 13000))
    print "_______________________________________________________________________________________"
    print "------------------------ BEACON SERVER DOWN Machines  ---------------------------------"
    print "_______________________________________________________________________________________"

    sock.send(query_beacon_server_down)
    query_io = ''

    while True:
        data = sock.recv(1024)
        query_io = '%s%s' % (query_io, data)

        if data == '' or 'rows selected' in data:
            break

    print query_io

execute_query_beacon_server_down()


def note_fun():
    print 'SPARE Machine Query Link : https://www.nocc.akamai.com/miniurl/?id=a95a \n\n\n'
    print 'NOTE : This is auto generated mail from System Operation Team. For any updates and responses please contact Shreeshail G(sgumgeri@akamai.com)\n'

note_fun()
