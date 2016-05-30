#!/bin/env python

import os
import time
import subprocess

from oni.utils import Util

class dns_ingest(object):


	def __init__(self,conf):

		self._initialize_members(conf)

	def _initialize_members(self,conf):

		self._collector_path = None
		self._hdfs_root_path = None
		self._queue_name = None
		self._pkt_num = None
		self._pcap_split_staging = None
		self._time_to_wait = None
		self._dsource='dns'

		# valdiate configuration info.
        conf_err_msg = "Please provide a valid '{0}' in the configuration file"
        Util.validate_parameter(conf['collector_path'],conf_err_msg.format("collector_path"))
        Util.validate_parameter(conf['queue_name'],conf_err_msg.format("queue_name"))
        Util.validate_parameter(conf['pkt_num'],conf_err_msg.format("pkt_num"))
        Util.validate_parameter(conf['pcap_split_staging'],conf_err_msg.format("pcap_split_staging"))
        Util.validate_parameter(conf['time_to_wait'],conf_err_msg.format("time_to_wait"))

		# set configuration.
		self._collector_path = conf['collector_path']
        self._hdfs_root_path = "{0}/{1}".format(conf['huser'] , self._dsource)
        self._time_to_wait = conf['time_to_wait']
		self._pkt_num = conf['pkt_num']
        self._pcap_split_staging = conf['pcap_split_staging']
        self._queue_name = conf['queue_name']

	def start(self):

		# process pcap files in collector path.
		print "Watching the path: {0} to collect files".format(self._collector_path)
		while True:
			for currdir, subdir,files in os.walk(self._collector_path):
				for file in files:
					if file.endswith('.pcap'):
						print "New file: {0}".format(file)
						file_full_path = os.path.join(currdir,file)
                        self._process_pcap_file(file,file_full_path,self._hdfs_root_path)
						print "file :{0} processed".format(file)
			time.sleep(self._time_to_wait)


	def _process_pcap_file(self,file_name,file_local_path,hdfs_root_path):

		# use edit cap to avoid time zone problems.
		name = file_name.split('.')[0]
		split_cmd="editcap -c {0} {1} {2}/{3}_oni.pcap".format(self._pkt_num,file_local_path,self._pcap_split_staging,name)
		print split_cmd
		subprocess.call(split_cmd,shell=True)
		
		#move files to hdfs
		for currdir,subdir,files in os.walk(self._pcap_split_staging):
			for file in files:
				if file.endswith(".pcap") and "{0}_split".format(name) in file:
				
					# get timestamp from the file name to build hdfs path.
					file_date = file_name.split('.')[0]
					pcap_hour = file_date[-6:-4]
					pcap_date_path = file_date[-14:-6]
										
					# hdfs path with timestamp.
					hdfs_path = "{0}/binary/{1}/{2}".format(hdfs_root_path,pcap_date_path,pcap_hour)
										
					# load file to hdfs.
					Util.load_to_hdfs(file,os.path.join(currdir,file),hdfs_path)
										
					#send rabbitmq notificaion.
					hadoop_pcap_file = "{0}/{1}".format(hdfs_path,file)
                    Util.send_new_file_notification(hadoop_pcap_file,self._queue_name)
				
		rm_big_file = "rm {0}".format(file_local_path)
        print rm_big_file
        subprocess.call(rm_big_file,shell=True)
		









