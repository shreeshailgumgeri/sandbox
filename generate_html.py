#!/usr/bin/env python

"""
Author - Shreeshail G
Date - 06 Mar 2016

This function is used to generate the HTML msg body which is to be sent on a day to day basis
"""

import smtplib
import time
import re
from time import gmtime, strftime
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText


class GenerateHTML:
	def __init__(self):
		# - Setting the sender and receiver Email ID's
		current_time = int(time.time())
		current_time = gmtime(current_time)
		human_readable_string = strftime("%d-%b-%Y", current_time)
		GenerateHTML.sender_emailID = "automon-media@akamai.com"
		#GenerateHTML.receiver_emailID = ["sgumgeri@akamai.com","bfakrudd@akamai.com"]
		#GenerateHTML.receiver_emailID = ["sanrao@akamai.com","akonar@akamai.com","sgumgeri@akamai.com","gyadav@akamai.com","bfakrudd@akamai.com","hshekhar@akamai.com"]
		GenerateHTML.receiver_emailID = ["dna-devtest@akamai.com","analyticsdb@akamai.com","dp-dev@akamai.com","mcs-blr@akamai.com","savasant@akamai.com","hkammana@akamai.com","lbabu@akamai.com","bfakrudd@akamai.com","gyadav@akamai.com","hshekhar@akamai.com","sobhatta@akamai.com"]
		
		#GenerateHTML.receiver_emailID = ["dna-devtest@akamai.com","adb-dev@akamai.com","dp-dev@akamai.com","savasant@akamai.com","hkammana@akamai.com","lbabu@akamai.com","bfakrudd@akamai.com","sgumgeri@akamai.com","asathnur@akamai.com","kmuddapo@akamai.com","jojose@akamai.com","hmuddapp@akamai.com","hshekhar@akamai.com"]
		GenerateHTML.header = "[Email Report: " + human_readable_string +"] COLORED SHOUTCAST : Data variation for last 5 days"

	def createMsgObj(self):
		GenerateHTML.msgObj = MIMEMultipart('alternative')
		GenerateHTML.msgObj['Subject'] = GenerateHTML.header
		GenerateHTML.msgObj['From'] = GenerateHTML.sender_emailID

		current_time = int(time.time())
		current_time = gmtime(current_time)
		human_readable_string = strftime("%d-%b-%Y %I:%M:%S %p", current_time)

		html = self.generateHTMLContent()

		# - The part1 ignored as it is just plain text. Plain text could not be attached
		part2 = MIMEText(html, "html")

		GenerateHTML.msgObj.attach(part2)

		server = smtplib.SMTP('127.0.0.1')

		for each_email in GenerateHTML.receiver_emailID:
			server.sendmail(GenerateHTML.sender_emailID, each_email, GenerateHTML.msgObj.as_string())

		server.quit()

	def generateHTMLContent(self):
		fp = open("data_variation.csv", "rU")
		lines_of_file = fp.readlines()
		fp.close()

		current_time = int(time.time())
		current_time = gmtime(current_time)
		human_readable_string = strftime("%d-%b-%Y %I:%M:%S %p", current_time)

		html_content = '<!DOCTYPE html>\n<html>\n<head>\n<title> Data Variation per Day </title>\n</head>\n<body>\n'
		html_content = html_content + "<p>NOTE : This Mail is sent to below Mailing list : dna-devtest@akamai.com; analyticsdb@akamai.com; dp-dev@akamai.com; mcs-blr@akamai.com; savasant@akamai.com; hkammana@akamai.com; lbabu@akamai.com; bfakrudd@akamai.com; gyadav@akamai.com; hshekhar@akamai.com; sobhatta@akamai.com <br /><br />Hi ALL,<br /><br />The data variation for selected customers across     pipelines as on "+human_readable_string+" is as follows<br /></p>\n"
		html_content = html_content + "<p> <font color=\"GREEN\" size=\"4\">COLORS Depiction :</font><br> <font color=\"ORANGE\" size=\"4\"> ORANGE  ==> 5 to 10 % DIFFERENCE Across Buddies </font><br> <font color=\"red\" size=\"4\">RED    ==> More than 10 %  DIFFERENCE Across Buddies </font></p><hr>"

		start_num = 0
		for each_line in lines_of_file:
			each_line = each_line.strip()
			if(re.search("^#", each_line)):
				if(start_num == 0):
					html_content = html_content + '<div style="float:left">\n'
					html_content = html_content + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"
					html_content = html_content + "<caption> " + each_line + " </caption>\n"
					start_num = start_num + 1
				else:
					html_content = html_content + "</table>\n<br />\n"
					html_content = html_content + "<table cellpadding='3' cellspacing='5' style='border:1px dashed black; border-collapse: collapse;'>\n"
					html_content = html_content + "<caption> " + each_line + " </caption>\n"
					start_num = start_num + 1
			elif((re.search("^adb", each_line)) or (re.search("^[0-9]+\.", each_line))):
				ips = each_line.split(",")
				html_content = html_content + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
				html_content = html_content + "<td style='border-right: 1px dashed black; border-collapse: collapse;'> Time </td>\n"
				for each_ip in ips:
					html_content = html_content + "<td style='border-right: 1px dashed black; border-collapse: collapse;'> " + each_ip.strip() + "</td>\n"

				html_content = html_content + "</tr>\n"
			else:
				elements = each_line.split(",")
				timestamp = int(elements[0])
				timestamp = gmtime(timestamp)
				date_to_string = strftime("%d-%b-%Y %I:%M:%S %p", timestamp)
				html_content = html_content + "<tr style='border:1px dashed black; border-collapse: collapse;'>\n"
				html_content = html_content + "<td style='border-right: 1px dashed black; border-collapse: collapse;'> " + date_to_string + "</td>\n"
				elements.reverse()
				unwanted_element = elements.pop()
				elements.reverse()

				for each_element in elements:
					# metric, percentage = each_element.strip().split("=")
					# metric = metric[2:]
					# percentage = percentage[1:-2] + "%"
					metric, percentage, percentage_across = each_element.strip().split("=")
					metric = metric[2:]
					#percentage = percentage[1:-2] + "%"
					percent = re.findall('\d+',percentage_across[1:-2])[0]
					percentage_across = percentage_across[1:-2] + "%"
					color = "WHITE"
					if abs(int(percent.strip())) > 5 and abs(int(percent.strip())) < 10 :
						color = "#FF9900"
					elif abs(int(percent.strip())) > 10 :
						color = "RED"

					html_content = html_content + "<td bgcolor="+color+" align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + metric + "( "+percentage+"% ) | B/W Rep: ( "+percentage_across+" )" + "</td>\n"
					# html_content = html_content + "<td align='left' style='border-right: 1px dashed black; border-collapse: collapse;'> " + metric + "( "+percentage+" )" + "</td>\n"

				html_content = html_content + "</tr>\n"

		html_content = html_content + "</table>\n<br /><p> Regards <br/>DNA OPS</p>\n</div>\n</body>\n</html>"
		print html_content

		return html_content

