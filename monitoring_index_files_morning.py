#!/usr/bin/env python

# - Standard python modules used by this script
import socket
import re
import datetime
import sys
import os
import subprocess
from collections import defaultdict
from itertools import izip_longest


def retrieve_ip_coast_label_from_query():
    """

    """
    host = "dna.dev.query.akadns.net"
    port = 13000
    timeout = 10
    query = ("select ip, label, replica from dna_machines where ROLE in "
             "('LPQE', 'FBLB', 'RTProc_FBLB') and status = 'ACTIVE';")
    conn = socket.create_connection( (host, port), timeout)
    num_bytes_sent = conn.send(query)
    if num_bytes_sent <= 0:
        print("Unable to send the query %s for execution" % query)
        sys.exit(-1)

    # - The variable query_data will hold all the data sent by recv()
    query_data = ""
    while True:
        try:
            data_received = conn.recv(4096)
            if data_received:
                query_data = "%s%s" % (query_data, data_received)
            else:
                break
        except socket.timeout:
            break

    return query_data


def parse_query_data(query_data):
    """

    """
    # - The structure of the dictionary machines would look like this
    # {
    #   "LABEL_1" : {
    #                   "REPLICA_1" : "IP_1"
    #                   "REPLICA_2" : "IP_2"
    #               }
    #   "LABEL_2" : {
    #                   "REPLICA_1" : "IP_1"
    #                   "REPLICA_2" : "IP_2"
    #               }
    # }
    machines = defaultdict(lambda: defaultdict(str))

    # - Only select query output lines that begin with a machine IP
    ip_regex = re.compile("^[0-9]+\..*$")

    # - To replace multiple occurrences of spaces and tabs with a single tab
    # - This is to ensure that the split is a simple split on the tab
    multiple_tabs = re.compile("[ \t]+")

    for each_line in query_data:
        each_line = each_line.strip()
        if ip_regex.match(each_line):
            ip, label, replica = re.sub(multiple_tabs, "\t", each_line).split("\t")

            # - Add all the machines that are to be discarded in this list
            if label in ["FBLB_I0", "FBLB_I1", "LPQE_100", "LPQE_N1", "LPQE_N2"]:
                continue
            # - The machine FBLB_I4 and RTPROC_FBLB_1 are the same compare them as such
            if label == "FBLB_I4":
                label = "RTPROC_FBLB_1"

            machines[label][replica] = ip

    return machines


def compare_index_files(machines):
    """

    """
    # - To replace multiple occurrences of spaces and tabs with a single tab
    # - This is to ensure that the split is a simple split on the tab
    multiple_tabs = re.compile("[ \t]+")

    # - Always compare yesterday's index files
    # - Yesterday's index files can never get changed
    yesterday_date_obj = datetime.date.today() - datetime.timedelta(days=1)

    # - The stats dictionary would look like this
    # {
    #   "LABEL_1" : {
    #                   "IP_1" : {
    #                               "BID_1" : Num files downloaded
    #                               "BID_2" : Num files downloaded
    #                            }
    #                   "IP_2" : {
    #                               "BID_1" : Num files downloaded
    #                               "BID_2" : Num files downloaded
    #                            }
    #               }
    #   "LABEL_2" : {
    #                   "IP_1" : {
    #                               "BID_1" : Num files downloaded
    #                               "BID_2" : Num files downloaded
    #                            }
    #                   "IP_2" : {
    #                               "BID_1" : Num files downloaded
    #                               "BID_2" : Num files downloaded
    #                            }
    #               }
    # }
    stats = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))

    # - This is to make sure that a month always has two digits
    month = int(yesterday_date_obj.month)
    if month < 10:
        month = "0%s" % (month)

    day = int(yesterday_date_obj.day)
    if day < 10:
        day = "0%s" %(day)
    file_name = "%s_%s_%s.idx" % (yesterday_date_obj.year, month,
                                  day)
    for each_label in machines:
        for each_coast in machines[each_label]:
            machine_ip = machines[each_label][each_coast]
            print("Working on IP - %s" % machine_ip)

            # - Since we are unable to execute the command in a standalone manner through the Popen call of subprocess we will
            # - create a shell script in the /tmp directory, execute this script and retrieve the output obtained
            with open('/tmp/index_files_comparision.sh', 'w') as fp:
                fp.write("#!/bin/bash\n")
                fp.write("SSH_AUTH_SOCK=/tmp/ssh-Y57qwnBgyqfV/agent.26342; export SSH_AUTH_SOCK;\n")
                #fp.write("%s -2 testgrp@%s -i /home/jadas/.ssh/keys/current/deployed/2015-06-19 'find %s -type f -name \"%s\" -exec wc -l {} \;'"
                #         % ("/usr/bin/gwsh", machine_ip, "/ghostcache/analytics/downloader/index_files/", file_name))
                #fp.write("%s -2 -o StrictHostKeyChecking=no testgrp@%s -i ~/OPS/deployed_key/sgumgeri-deployed-2016-12-29.ppk 'find %s -type f -name \"%s\" -exec wc -l {} \;'"
                #         % ("/usr/local/bin/gwsh", machine_ip, "/ghostcache/analytics/downloader/index_files/", file_name))
                fp.write("%s testgrp@%s 'find %s -type f -name \"%s\" -exec wc -l {} \;'"
                         % ("/usr/local/bin/gwsh", machine_ip, "/ghostcache/analytics/downloader/index_files/", file_name))

            # - To make sure that the file has execute permissions
            os.chmod('/tmp/index_files_comparision.sh', 0777)
            print("Executing the shell script /tmp/index_files_comparision.sh")
            sobj = subprocess.Popen(["/bin/bash", "/tmp/index_files_comparision.sh"],
                                    stdout = subprocess.PIPE, stderr = subprocess.PIPE)
            stdout, stderr = sobj.communicate()

            # - Parse the contents of stdout and populate the stats dictionary
            stdout = stdout.split("\n")
            for each_output_line in stdout:
                each_output_line = each_output_line.strip()
                if each_output_line:
                    num_files, index_files = re.sub(multiple_tabs, "\t", each_output_line).split("\t")
                    beacon_id = index_files.split("/")[-2]
                    ip_and_coast = '%s_%s' % (machine_ip, each_coast)
                    stats[each_label][ip_and_coast][beacon_id] = int(num_files)

    return stats


def find_difference(machines, stats):
    """

    """
    # - This dictionary will hold the differences amongst the replicas
    difference = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    for each_label in machines:
        machine_ips = []
        for each_coast in machines[each_label]:
            machine_ips.append("%s_%s" % (machines[each_label][each_coast], each_coast))

        # - Perform the comparision for keys from both the dictionary
        for each_bid in stats[each_label][machine_ips[0]]:
            if abs(stats[each_label][machine_ips[0]][each_bid] - stats[each_label][machine_ips[1]][each_bid]) >= 0:
                difference[each_label][machine_ips[0]][each_bid] = stats[each_label][machine_ips[0]][each_bid]
                difference[each_label][machine_ips[1]][each_bid] = stats[each_label][machine_ips[1]][each_bid]

        for each_bid in stats[each_label][machine_ips[1]]:
            if abs(stats[each_label][machine_ips[0]][each_bid] - stats[each_label][machine_ips[1]][each_bid]) >= 0:
                difference[each_label][machine_ips[0]][each_bid] = stats[each_label][machine_ips[0]][each_bid]
                difference[each_label][machine_ips[1]][each_bid] = stats[each_label][machine_ips[1]][each_bid]

    return difference


if __name__ == "__main__":
    query_data = retrieve_ip_coast_label_from_query()
    query_data = query_data.split("\n")
    machines = parse_query_data(query_data)
    #machines = {"LPQE_0" : {"EAST" : "184.84.240.37", "CENTRAL" : "184.51.203.4"},
    #           "RTPROC_FBLB_3" : {"EAST" : "184.51.103.129", "CENTRAL" : "96.7.250.72"}}

    #machines = {"RTPROC_FBLB_5": {"EAST" : "184.51.103.138", "CENTRAL" : "184.51.203.53"}}

    stats = compare_index_files(machines)
    difference = find_difference(machines, stats)

    # - Print the differences
    with open("/tmp/difference_list.txt", "w") as fp:
        for each_label in difference:
            fp.write("%s\n" % each_label)
            machine_ips = []
            for each_ip in difference[each_label]:
                machine_ips.append(each_ip)

            fp.write("%s\n" % ",".join(machine_ips))

            for each_bid in difference[each_label][machine_ips[0]]:
                fp.write("%s - %d, %d\n" % (each_bid, difference[each_label][machine_ips[0]][each_bid],
                                            difference[each_label][machine_ips[1]][each_bid]))
