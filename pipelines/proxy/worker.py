#!/bin/env python

import logging
from oni.utils import Util


class Worker(object):

    def __init__(self,db_name,hdfs_app_path,kafka_topic):
        self._initialize_members(db_name,hdfs_app_path,kafka_topic)

    def _initialize_members(db_name,hdfs_app_path,kafka_topic):
        
        # get logger instance.
        self._logger = logging.getLogger("ONI.INGEST.PROXY.WORKER")
        self._db_name = db_name
        self._hdfs_app_path = hdfs_app_path
        self._kafka_topic = kafka_topic

    
    def start(self):

        self._logger.info("Creating Spark Job for: {0}".format(self._kafka_topic.Topic))

                

        # spark job command.
        spark_job_cmd = ("spark-submit --master yarn"
                        " --jars spark-streaming-kafka-0-8-assembly_2.11-2.0.0.jar"
                        " parse_proxy.py {0} {1}".format(zookeper, topic))

        
        # start spark job.
        Utils.execute_cmd(spark_job_cmd)

        