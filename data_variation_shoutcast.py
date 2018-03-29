#!/usr/bin/env python

"""
Author - Shreeshail G
Date - 06 Mar 2016

This program is used to find the data variation for the last three days (for the time being) for a given metric.
Given as input a configuration file that contains details like the metric to calculate data against, the IP's against which to calculate data, the     analyzer and table ID's the script should go ahead and calculate the value of the metric for the day against all the IP's and the value for the last   three days and alert the differences to the person concerned
"""

# - Standard import files
import os
import sys
import time
import re
import socket
from optparse import OptionParser
from collections import OrderedDict

# - Appending the current path
try:
	sys.path.append(".")
	import generate_html as gh
except ImportError,e:
	print("The required HTML generator library could not be imported")
	sys.exit(-1)

"""
a.> OS module for performing whether the file exists at the specified path
b.> SYS module for performing an exit if the conditions are not suitable for script execution
c.> TIME module for getting the system time
d.> RE module for performing regular expression checks
e.> SOCKET module for retrieving the data over a socket
f.> OPTPARSE for parsing command-line arguments
"""

# - The class that drives it all
class DataVariation:
	def __init__(self):
		"""
		This function does nothing for the time being
		"""
		pass

	def parseCommandLineArgs(self):
		"""
		This function is used to parse command-line arguments and also call the getMetricsToQuery function
		"""

		DataVariation.cmdline_args = OptionParser()
		DataVariation.cmdline_args.add_option("-c", "--config_file", dest="config_file", default="./dv_shoutcast.config", help="The path to the configuration file")

		(options, args) = DataVariation.cmdline_args.parse_args()

		# - Checking whether the specified config file is present
		if not os.path.isfile(options.config_file):
			print("The configuration file is not present at the specified path")
			sys.exit(-1)

		DataVariation.config_file = options.config_file

		# - Preparing the CSV file. This is used to flush out any data if it is already present
		fp = open("data_variation.csv", "w")
		fp.close()

		# - Parse each line of the config file
		self.getMetricsToQuery()

	def getMetricsToQuery(self):
		"""
		This function is used to get the details necessary to construct the query
		"""

		# - Opening the file which contains all the configuration
		fp = open(DataVariation.config_file, "rU")

		# - Reading each line present in the file and performing the operation
		try:
			while(True):
				line = fp.readline().strip()
				if(line == ""):
					break

				# - Ignore comments. Assuming that comments begin with a '#' sign
				if(re.search("^#", line)):
					pass
				else:
					# - Extracting the required information
					pipeline, service_name, aid, tid, metric, ips = line.split(":")

					# - Days to consider
					days = 5

					# - The dictionary to be sent
					query_essentials = {"service": service_name, "aid" : aid, "tid" : tid, "metric" : metric, "ips" : ips, "days" : days, "pipeline": pipeline}

					# - Call the function to get the details required
					self.executeQuery(query_essentials)
		except Exception,e:
			print str(e)

	def executeQuery(self, query_essentials):
		"""
		Construct the query and execute the details
		"""
		# - Extracting the essentials
		service_name = query_essentials["service"]
		pipeline = query_essentials["pipeline"]
		aid = str(query_essentials["aid"])
		tid = str(query_essentials["tid"])
		gdn = service_name + "." + aid + "." + tid
		metric = query_essentials["metric"]
		days = query_essentials["days"]
		ips = query_essentials["ips"].split(",")

		# - Getting the time
		time_to_consider = int(time.time())
		to_time = time_to_consider - (time_to_consider % 86400)
		from_time = str(to_time - (86400 * days))
		to_time = str(to_time)
		t = int(from_time)
		tdict = OrderedDict()

		while t < int(to_time) :
			tdict[str(t)] = "No Data"
			t+=86400

		#print 'TIME list : ' + str(tdict)
		# - Constructing the query
		query = "Select time - ((time - "+from_time+") % 86400), "+metric+" From "+gdn+" Duration time >= "+from_time+" and time < "+(to_time)+" Group by 1 Order by 1 ASC ;"

		# - All data holder
		all_data_collection = {}
		print "=============================================================================================================================================================================="
		# - Going over all the IPS for a metric
		for ip in ips:
			try:
				retries = False
				num_retries = 3

				# - List holding all the data
				data_holder = []

				while(not retries):
					print "-----------------------------------------------------\n IP :" +ip
					print "QUERY : " +query
					while t < int(to_time) :
						tdict[str(t)] = "No Data"
						t+=86400
					sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
					sock.connect((ip, 8160))
					sock.send(query + "\n")

					data = ""
					from_sock = ""
					while(1):
						from_sock = sock.recv(1024)
						if(from_sock == ""):
							break
						else:
							data = data + from_sock
					print 'Data :' + str(len(data.split('\n')))
					data_len = len(data.split('\n'))

					if(re.search("^", data)):
						retries = False
						if(num_retries > 0):
							num_retries = num_retries - 1
						else:
							retries = True
							data = "No data"
					else:
						retries = True

				if(data != "No data"):
					
					if data_len == 8 :
						lines = data.split("\n")
						for line in lines:
							
							if(re.search("^i", line)):
								temp_arr = line.split("")
								#for key in tdict.keys():
								epoch_time = temp_arr[1]
								data_metric = temp_arr[2]
								key = str(temp_arr[1])
								value = str(temp_arr[2]) 
								tdict[key] = value
								print key + '-->' + value
						for key,value in tdict.items():
							data_holder.append((key,value)) 
						print data_holder 

						differences = self.calculate_differences(data_holder,days)
						all_data_collection[ip] = differences
					elif data_len < 8 :
						lines = data.split("\n")
						for line in lines:
							while t < int(to_time) :
								tdict[str(t)] = "No Data"
								t+=86400
							if(re.search("^i", line)):
								temp_arr = line.split("")
								#for key in tdict.keys():
								epoch_time = temp_arr[1]
								data_metric = temp_arr[2]
								key = str(temp_arr[1])
								value = str(temp_arr[2])
								tdict[key] = value
								print key + '-->' + value
						
						for key,value in tdict.items():
							if "No" in value:
								print value
								data_holder.append((key,'0'))
							else :
								data_holder.append((key,value))

						print data_holder
							
						differences = self.calculate_differences(data_holder,days)
						all_data_collection[ip] = differences
				else:
					while t < int(to_time) :
						tdict[str(t)] = "No Data"
						t+=86400
					print 'IM in else case now'
					for key,value in tdict.items():
						if "No" in value:
							print value
							data_holder.append((key,'0'))
						else :
							data_holder.append((key,value))

					print data_holder
						
					differences = self.calculate_differences(data_holder,days)
					all_data_collection[ip] = differences

			except socket.error,e:
				print 'IM in Exception case now'
				print 'Here is the 1st Exception '+ str(e)
				for key,value in tdict.items():
					if "No" in value:
						print value
						data_holder.append((key,'0'))
					else :
						data_holder.append((key,value))

				print data_holder
						
				differences = self.calculate_differences(data_holder,days)
				all_data_collection[ip] = differences
				
				pass
				
				# num_days = int(days)
				# data_to_append = []
				# timestamp = int(from_time)
				# first_entry = True

				# while(num_days != 0):
				# 	if(first_entry):
				# 		data_to_append.append((str(timestamp), "No data", "-"))
				# 		first_entry = False
				# 	else:
				# 		timestamp = timestamp + 86400
				# 		data_to_append.append((str(timestamp), "No data" ,"-"))
				# 	num_days = num_days - 1

				# all_data_collection[ip] = data_to_append
				# pass

			except Exception,e:
				print 'Here is the 2nd Exception '+ str(e)
				pass

		# - Display the data in the format that you want
		self.displayData(all_data_collection, metric, gdn, pipeline, ips)

	def displayData(self, all_data_collection, metric, gdn, pipeline, ips):
		"""
		This function is used to create the CSV dump of the data needed to be displayed.
		"""
		fp = open("data_variation.csv", "a")

		# - Writing the metric name
		fp.write("# "+pipeline+" "+gdn+" "+metric+"\n")

		# - Join them all with commas
		line_to_write = ",".join(ips)

		# - Writing the IPS
		fp.write(line_to_write + "\n")

		# - Collecting data based on the epoch
		epoch_data = {}

		# - Epoch for ordering
		epoch = []

		for ip in ips:
			value = all_data_collection[ip]
			for subvalue in value:
				try:
					epoch_data[subvalue[0]].append((subvalue[1], subvalue[2]))
				except KeyError,e:
					epoch_data[subvalue[0]] = []
					epoch_data[subvalue[0]].append((subvalue[1], subvalue[2]))
					epoch.append(subvalue[0])

		# - Sorting the time
		epoch.sort()

		# - Reversing the order of the elements
		# epoch.reverse()
                def getPercentage(data, max):
                        if data == 'No data':
                                return 100 
                        elif int(max) == 0 :
                                return 0
                        else:
                                return (max - int(data)) * 100.0 / float(max)

                def convertValue(value):
                        if value[0] == 'No data':
                                return 0
                        else:
                                return int(value[0])

                for individual_epoch in epoch:
                        line_to_write = individual_epoch + "," 
                        list_of_values = epoch_data[individual_epoch]
                        #print list_of_values
                        max_replica = max(map(convertValue, list_of_values))
                        delta = [getPercentage(value[0], max_replica) for value in list_of_values]

                        for i, each_value in enumerate(list_of_values):
                                line_to_write += "( {0} = {1} = {2:.2f} ),".format(each_value[0], each_value[1], delta[i])

                        fp.write(line_to_write[:-1] + "\n")

		# for individual_epoch in epoch:
		# 	line_to_write = individual_epoch + ","
		# 	list_of_values = epoch_data[individual_epoch]

		# 	for each_value in list_of_values:
		# 		line_to_write = line_to_write + "( " + each_value[0] + " = " + str(each_value[1]) + " ),"

		# 	fp.write(line_to_write[:-1] + "\n")

		fp.close()

	def calculate_differences(self, data_holder,days):
		"""
		This function is used to calculate the differences between successive days
		"""

		# - New data structure for holding the differences
		differences = []

		# - Getting the array pass
		length_of_data_holder = len(data_holder) - 1
		# - Start num for traversing the array
		i = 0

		# - The initial element in the array
		differences.append((data_holder[0][0], data_holder[0][1], 0))

		# - Calculating the difference
		while(length_of_data_holder != 0):
			start_element = data_holder[i]
			end_element = data_holder[i+1]
			if float(start_element[1]) != 0:
				percentage_change = int(((float(end_element[1]) - float(start_element[1])) / float(start_element[1])) * 100)
			else :
				percentage_change = 100
			new_tuple = (end_element[0], end_element[1], percentage_change)
			differences.append(new_tuple)
			i = i + 1
			length_of_data_holder = length_of_data_holder - 1

		return differences

# - The main method of the program
if __name__ == "__main__":
	# - Calculating the data and dumping them in a CSV file
	dbObj = DataVariation()
	dbObj.parseCommandLineArgs()

	# - Generating the HTML body
	htmlobj = gh.GenerateHTML()
	htmlobj.createMsgObj()

