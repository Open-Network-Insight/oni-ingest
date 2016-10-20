#!/bin/env python

import logging
import json
import os
import sys
import copy
from oni.utils import Util, NewFileEvent
from multiprocessing import Pool, Process
from oni.file_collector import FileWatcher
import time



class Collector(object):
    
    def __init__(self,hdfs_app_path,kafka_topic,conf_type):
        
        self._initialize_members(hdfs_app_path,kafka_topic,conf_type)
        
    def _initialize_members(self,hdfs_app_path,kafka_topic,conf_type):
        
        # getting parameters.
        self._logger = logging.getLogger('ONI.INGEST.PROXY')
        self._hdfs_app_path = hdfs_app_path
        self._kafka_topic = kafka_topic

        # get script path
        self._script_path = os.path.dirname(os.path.abspath(__file__))

        # read proxy configuration.
        conf_file = "{0}/ingest_conf.json".format(os.path.dirname(os.path.dirname(self._script_path)))
        conf = json.loads(open(conf_file).read())
        self._message_size = conf["kafka"]["message_size"]
        self._conf = conf["pipelines"][conf_type]

        # get collector path.
        self._collector_path = self._conf['collector_path']

        #get supported files
        self._supported_files = self._conf['supported_files']

        # create collector watcher
        self._watcher = FileWatcher(self._collector_path,self._supported_files)

        # Multiprocessing. 
        self._processes = 5
        self._ingestion_interval = 5
        self._pool = Pool(processes=self._processes) 


    def start(self):

        self._logger.info("Starting PROXY INGEST")
        self._watcher.start()
   
        try:
            while True:         
                if self._watcher.HasFiles: self._process_files()
                time.sleep(self._ingestion_interval)                   

        except KeyboardInterrupt:  
                  
            self._logger.info("Stopping Proxy collector...")           
            self._watcher.stop()
            Util.remove_kafka_topic(self._kafka_topic.Zookeeper,self._kafka_topic.Topic,self._logger)   

    def _process_files(self):        
        
        kafka_client = self._kafka_topic
        logger = self._logger
        msg_size = self._message_size
        p_list = []

        for x in range(1,self._processes):
            file = self._watcher.GetNextFile()  
            p = Process(target=self._ingest_file,args=(file,))
            p.start()
            p_list.append(p)         
            if  not self._watcher.HasFiles: break
        
        for process in p_list:process.join()

    def _ingest_file(self,file):

        self._logger.info("Ingesting new file") 
        self._logger.info("Sending new file to kafka; topic: {0}".format(self._kafka_topic.Topic)) 
        message = ""
        with open(file,"rb") as f:

            for line in f:
                message += line
                if len(message) > self._message_size:                    
                    kafka_client.send_message(message,self._kafka_topic.Partition)
                    message = ""

            #send the last package.
            self._kafka_topic.send_message(message,self._kafka_topic.Partition)
        rm_file = "rm {0}".format(file)
        Util.execute_cmd(rm_file,logger)
        self._logger.info("File {0} has been successfully sent to Kafka Topic:{1}".format(file,self._kafka_topic.Topic))

