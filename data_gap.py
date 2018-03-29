#!/usr/bin/env python
import os
import time
import socket
import re
from datetime import datetime
from optparse import OptionParser
from collections import namedtuple
from itertools import groupby, izip_longest
from multiprocessing.pool import ThreadPool

query_node_mapping = { 'VA:1':'adbq1.qosm.east1.dna.akamai.com',
                       'VA:2':'adbq1.qosm.east2.dna.akamai.com',
                       'SJC:1':'adbq2.qosm.central1.dna.akamai.com',
                       'SJC:2':'adbq2.qosm.central2.dna.akamai.com' }

qosm_query_node_mapping = { 'DFW:1':'adbq1.qosm.dfw1.dna.akamai.com',
                            'IAD:1':'adbq1.qosm.iad1.dna.akamai.com',
                            'SJC:1':'adbq1.qosm.sjc1.dna.akamai.com' }

class GDNNamedTuple(namedtuple('GDNNamedTuple', ['service_name', 'analyzer_id', 'table_id'])):
  __slots__ = ()

  def __str__(self):
    return "{0}.{1}.{2}".format(self.service_name, self.analyzer_id, self.table_id)

class StatNamedTuple(namedtuple('StatNamedTuple', ['delta_percent', 'replica', 'host', 'num_records', 'num_updates'])):
  __slots__ = ()
        
  def __str__(self):
    return "{0} {1:>7} {2:>5.1f}%".format(re.sub('.dna.akamai.com', '', self.host), 
                                          self.num_records, self.delta_percent)

class StatQueryNamedTuple(namedtuple('StatQueryNamedTuple', ['service_name', 'analyzer_id', 'table_id', 'start', 'end'])):
  __slots__ = ()

  def __str__(self):
    return adb_stat_query_template.format(self.service_name, self.analyzer_id, self.table_id, self.start, self.end)

secs_in_day = 24 * 3600

query2_template = "select service_name, analyzer_id, table_id, min(start_day), max(end_day) from adb_table_mon where {0} end_day > current_timestamp - (86400 * 45) and start_day < current_timestamp + 86400 group by 1,2,3 order by 1,2,3;"

adb_stat_query_template = "stat {0}.{1}.{2} duration time >= {3} and time < {4};\n"

adb_partial_sync_template = "/usr/local/akamai/bin/python2.7 /usr/local/akamai/analyticsdb/bin/adb_partial_sync.py -S {0} -D {1} -s {2} -a {3} -t {4} -l {5} -u {6}"

adb_pat = re.compile(r'(adb\S+com)')

pool = ThreadPool(processes=4)

def percentage(part, whole):
  if 0 == whole:
    return 0
  else:
    return (100.0 * part) / whole

def mkdate(epoch):
  return datetime.fromtimestamp(epoch).strftime("%Y/%m/%d")

def parseArgs():
  parser = OptionParser()
  parser.add_option("-g", "--gdn", dest="gdn_pattern", help="[REQUIRED] GDN pattern, E.g.: ma.1.2, ma.1.*, ma.*")
  parser.add_option("-d", "--days", dest="days", help="No. of days (default : 1)", type="int", default=1)
  parser.add_option("-b", "--bucket", action="store_true", dest="bucket", help="Bucketize data per day (default: False)", default=False)
  parser.add_option("-p", "--min_delta_percent", dest="min_delta_percent", help="Output rows with difference in data between replicas >= min_delta_percent (default: 0)", type="int", default=0)
  parser.add_option("-n", "--min_num_records", dest="min_num_records", help="Output rows with num_records in a replica >=  min_num_records (default: 0)", type="int", default=0)
  parser.add_option("-s", "--print_sync_cli", action="store_true", dest="print_sync_cli", help="Print adb_partial_sync command (default : False)", default=False)
  (options,args) = parser.parse_args()
  if not options.gdn_pattern:
    parser.error("GDN pattern not given")
  return options

def build_query2_str(GDN):
  where_clause = ''
  if GDN.service_name != "*":
    where_clause += ' service_name = "{0}" '.format(GDN.service_name)

  if GDN.analyzer_id != "*":
    if where_clause:
      where_clause += " and "
    where_clause += 'analyzer_id = "{0}" '.format(GDN.analyzer_id)

  if GDN.table_id != "*":
    if where_clause:
      where_clause += " and "
    where_clause += 'table_id = "{0}" '.format(GDN.table_id)

  if where_clause:
    where_clause += " and "

  return query2_template.format(where_clause)

def get_query_response(query, node, port, print_status=True):
  """Run query on node and return response."""
  status = node
  response_string = ''
  sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
  try:
    sock.connect((node, port))
    sock.settimeout(5)
    sock.send(query)
    while(1):
      data = sock.recv(4096)
      response_string += data
      if not data or data.endswith('rows selected'):
        break
  except socket.timeout:
    status += ' : TIMEOUT'
  except socket.error, ex:
    status += ' : ERROR ' + str(ex)
  finally: 
    sock.close()
  
  if response_string:
    status += ' : SUCCESS'
  
  if print_status:
    print status
  return response_string.split('\n')

def gen_parse_response(lines, replica):
  """Parse lines in response of stat query and generate sequence of StatNamedTuples."""
  n_tuples = 0
  for line in lines:
    if not line or line.startswith('ip'):
      continue
    elif not line.startswith('\x01\x01\x01'):
      stat = line.split('\x01')
      n_tuples += 1
      yield StatNamedTuple(0, replica, stat[0], *map(int, stat[2:]))
    else:
      host_match = adb_pat.search(line)
      if host_match:
        host = host_match.group(0) + "*"
        n_tuples += 1
        yield StatNamedTuple(0, replica, host, 0, 0)

  if not n_tuples:
    yield StatNamedTuple(0, replica, '-', 0, 0)

def gen_groupby_host(stat_tuples, replica):
  """Generate seq of grouped StatNamedTuples by hosts and aggregate num_records from a sorted seq of StatNamedTuples."""
  for host, stat_tuples in groupby(stat_tuples, lambda stat_tuple: stat_tuple.host):
    metrics = ((stat_tuple.num_records, stat_tuple.num_updates) for stat_tuple in stat_tuples)
    agg_records, agg_updates = reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]), metrics)
    yield StatNamedTuple(0, replica, host, agg_records, agg_updates)

def groupby_replica(stat_tuples, replica):
  """Group sorted seq of StatNamedTuples by replica and aggregate num_records."""
  metrics = ((stat_tuple.num_records, stat_tuple.num_updates) for stat_tuple in stat_tuples)
  agg_records, agg_updates = reduce(lambda x, y: (x[0] + y[0], x[1] + y[1]), metrics)
  return StatNamedTuple(0, replica, replica, agg_records, agg_updates)

def get_max_records(stat_tuples):
  """Get max no. of records in a replica and it's StatNamedTuple."""
  max_records, max_stat_tuple = max((stat_tuple.num_records, stat_tuple) for stat_tuple in stat_tuples
                                     if stat_tuple)
  return max_records, max_stat_tuple

def get_delta_percent(stat_tuple, max_records):
  if stat_tuple:
    return percentage(max_records - stat_tuple.num_records, max_records)
  else:
    return 0

def get_stat_tuple(delta_percent, stat_tuple):
  """Create StatNamedTuple from existing one with a given delta_percent.
     Also handles case where StatNamedTuple is None.
  """
  if stat_tuple:
    return StatNamedTuple(delta_percent, *stat_tuple[1:])
  else:
    return StatNamedTuple(0, '-', '-', 0, 0)

def get_stat_worker(arg):
  return get_stat_worker_(*arg)

def get_stat_worker_(replica, query_node, stat_query, print_status):
  """Fire stat query on query_node, parse result into StatNamedTuple seq."""
  response_lines = get_query_response(stat_query, query_node, 8159, print_status)
  # Convert response of stat query into a sorted (by replica, host) sequence of StatNamedTuples. 
  stat_tuples = sorted(gen_parse_response(response_lines, replica), 
                       key=lambda stat_tuple: (stat_tuple.replica, stat_tuple.host))
  return replica, stat_tuples

def print_formatted_row(date, gdn, row):
  print "{0} {1}   {2}".format(date, gdn, ' '.join('{0:>30}'.format(stat_tuple) for stat_tuple in row))

def print_grouped_replicas(date, gdn, grouped_replicas):
  """Print records aggregated at replica level."""
  print "-" * 165
  max_records, max_stat_tuple = get_max_records(grouped_replicas)
  tuple_row = (StatNamedTuple(get_delta_percent(stat_tuple, max_records), *stat_tuple[1:])
                 for stat_tuple in grouped_replicas)
  print_formatted_row(date, gdn, tuple_row)
  print "-" * 165

def print_grouped_hosts(date, gdn, grouped_hosts, min_delta_percent, min_num_records, print_cli):
  """Print records aggregated at host level."""
  for row in izip_longest(*grouped_hosts):
    max_records, max_stat_tuple = get_max_records(row)
    tuple_row = list()
    print_row = False
    for stat_tuple_ in row:
      delta_percent = get_delta_percent(stat_tuple_, max_records)
      stat_tuple = get_stat_tuple(delta_percent, stat_tuple_)
      tuple_row.append(stat_tuple)
      if delta_percent >= min_delta_percent and stat_tuple.num_records >= min_num_records:
        print_row = True
        if print_cli and delta_percent and stat_tuple.num_records:
          print adb_partial_sync_template.format(max_stat_tuple.host, stat_tuple.host, 
            gdn.service_name, gdn.analyzer_id, gdn.table_id, stat_query.start, stat_query.end)
    
    if print_row: 
      print_formatted_row(date, gdn, tuple_row)


def print_grouped_results(date, stat_query, gdn, min_delta_percent, min_num_records, print_cli):
  print_status = (0 == min_delta_percent and 0 == min_num_records)
  if print_status:
    print "-" * 165
    print stat_query
  args = ((replica, query_node, str(stat_query), print_status) 
           for replica, query_node in query_node_mapping.iteritems())
  results = pool.map(get_stat_worker, args)
  
  grouped_hosts = list()
  grouped_replicas = list()
  for replica, stat_tuples in results:
    grouped_replicas.append(groupby_replica(stat_tuples, replica))
    grouped_hosts.append(gen_groupby_host(stat_tuples, replica))

  if print_status:
    print_grouped_replicas(date, gdn, grouped_replicas)
  
  print_grouped_hosts(date, gdn, grouped_hosts, min_delta_percent, min_num_records, print_cli)


def gen_stat_query_strings(gdn, start_time, end_time):
  """Generate a sequence of StatQueryNamedTuple on per-day interval for given gdn, start time and end time."""
  for start in range(start_time, end_time, secs_in_day):
    yield StatQueryNamedTuple(gdn.service_name, gdn.analyzer_id, gdn.table_id,
                                start, start + secs_in_day)

def get_gdns_from_query2(input_gdn):
  query2_str = build_query2_str(input_gdn)
  print query2_str
  adb_table_mon_result = get_query_response(query2_str, 'dna.dev.query.akadns.net', 13000, False)
  return (GDNNamedTuple(*line.split()[:3]) for line in adb_table_mon_result[2:-3])
        

if __name__ == '__main__':
  options = parseArgs()
  input_gdn = GDNNamedTuple(*options.gdn_pattern.split('.'))
  days = options.days
  current_time = int(time.time())
  end_time = current_time - (current_time % secs_in_day)
  start_time = end_time - days * secs_in_day
  
  gdns = get_gdns_from_query2(input_gdn)
  for gdn in gdns:
    if options.bucket:
      dates = map(mkdate, xrange(start_time, end_time, secs_in_day))
      stat_querys = gen_stat_query_strings(gdn, start_time, end_time)
    else:
      start_str = mkdate(start_time)
      end_str = mkdate(end_time - 1)
      dates = [start_str + " - " + end_str]
      stat_querys = [StatQueryNamedTuple(gdn.service_name, gdn.analyzer_id, gdn.table_id,
                                                           start_time, end_time)]
    for date, stat_query in zip(dates, stat_querys):
      print_grouped_results(date, stat_query, gdn, options.min_delta_percent, options.min_num_records, options.print_sync_cli)
