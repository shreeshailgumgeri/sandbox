#!/usr/bin/env python

# - Standard Python modules
from collections import defaultdict

with open("/tmp/difference_list.txt", "r") as fp:
    lines = fp.readlines()

# - Remove "\n" at the end of a line
lines = map(str.strip, lines)

# - The allowed difference make it configurable
allowed_percent_diff = 20

stats = defaultdict(lambda: defaultdict(lambda: defaultdict (int)))
# - Parse through lines and populate
for each_line in lines:
    if each_line.find("FBLB") != -1:
        machine_label = each_line.strip()
    elif each_line.find("LPQE") != -1:
        machine_label = each_line.strip()
    elif each_line.find("CENTRAL") != -1:
        ip_1, ip_2 = map(str.strip, each_line.split(","))
    else:
        bid, num_files_2 = each_line.split(",")
        bid, num_files_1 = bid.split("-")
        bid, num_files_1, num_files_2 = map(str.strip, [bid, num_files_1, num_files_2])
        num_files_1, num_files_2 = map(int, [num_files_1, num_files_2])
        try:
            if abs((num_files_1 - num_files_2) * 100) / num_files_1 > allowed_percent_diff and abs(num_files_1 - num_files_2) > 10:
                stats[machine_label][ip_1][bid] = num_files_1
                stats[machine_label][ip_2][bid] = num_files_2
                """
                print THIS IS AT MOST PRIORITY PLEASE LOOK INTO IT
                print machine_label
                print "%s, %s" % (ip_1, ip_2)
                print "%s - %d, %d, %d" % (bid, num_files_1, num_files_2, abs((num_files_1 - num_files_2) * 100) / num_files_1)
                """
        except ZeroDivisionError, e:
            stats[machine_label][ip_1][bid] = num_files_1
            stats[machine_label][ip_2][bid] = num_files_2

for each_label in stats:
    print("%s" % each_label)

    machine_ips = []
    for each_ip in stats[each_label]:
        machine_ips.append(each_ip)

    print("%s" % ",".join(machine_ips))

    for each_bid in stats[each_label][machine_ips[0]]:
        x1 = stats[each_label][machine_ips[0]][each_bid] 
        x2 = stats[each_label][machine_ips[1]][each_bid]
        try :
            print("%s - %d, %d, %d%s " % (each_bid, stats[each_label][machine_ips[0]][each_bid],
                               stats[each_label][machine_ips[1]][each_bid], float((abs(x1 - x2)*100)/(x1)),'%'))
        except ZeroDivisionError :
            print "************ZeroDivisionError**********"
            print("%s - %d, %d, 100%%" % (each_bid, stats[each_label][machine_ips[0]][each_bid],stats[each_label][machine_ips[1]][each_bid]))
            print "***************************************"
            pass

print '\nNOTE : This is auto generated mail from System Operation Team. For any updates and responses please contact DNA OPS team\n'
