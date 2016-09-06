#!/bin/env python

import logging
import json
import os
import sys
import copy
from oni.utils import Util, NewFileEvent
from multiprocessing import Process
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
        self._conf = conf["pipelines"][conf_type]

        # get collector path.
        self._collector_path = self._conf['collector_path']

        # create collector watcher
        self._watcher =  Util.create_wathcher(self._collector_path,NewFileEvent(self),self._logger)

    def start(self):
        
        self._logger.info("Starting PROXY ingest")
        self._logger.info("Watching: {0}".format(self._collector_path))    
        self._watcher.start()

        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            self._logger.info("Stopping PROXY collector...")
            self._watcher.stop()
            self._watcher.join()
            
    
    def new_file_detected(self,file):
        
        self._logger.info("-------------------------------------- New File detected --------------------------------------")        
        self._logger.info("File: {0}".format(file))

        # get supported file extensions from configuration file.
        supported_files = self._conf['supported_files']        
        if file.endswith(tuple(supported_files)):

            self._logger.info("Sending new file to kafka; topic: {0}".format(self._kafka_topic.Topic))
            p = Process(target=self._ingest_file,args=(file,))
            p.start()
            
        else:
            self._logger.warning("File extension not supported: {0}".format(file))
            self._logger.warning("File won't be ingested")


    def _ingest_file(self,file):
                
        message = ""
        with open(file,"rb") as f:
            
            for line in f:                
                message += line            
                if len(message) > 999999:                                         
                    self._kafka_topic.send_message(message,self._kafka_topic.Partition)                   
                    message = ""                    
            # send the last package.
            self._kafka_topic.send_message(message,self._kafka_topic.Partition)              
        self._logger.info("File {0} has been successfully sent to Kafka Topic:{1}".format(file,self._kafka_topic.Topic))
    




                



        

        



        

        