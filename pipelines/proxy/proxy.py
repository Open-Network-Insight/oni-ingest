#!/bin/env python

import logging

class Collector(object):
    
    def __init__(self,hdfs_app_path,kafka):
        
        self._initialize_members(hdfs_app_path,kafka)
        
    def _initialize_members(self,hdfs_app_path,kafka):
        
        # getting parameters.
        self._logger = logging.getLogger('ONI.INGEST.PROXY')
        self._hdfs_app_path = hdfs_app_path
        self.kafka = kafka

    def start(self):
        
        self._logger.info("Starting proxy ingest")
        

        