#!/bin/env

import logging
class Kafka(object):


    def __init__(self,server,port,topic,partitions):

        self._initialize_members(server,port,topic,partitions)

    def _initialize_members(self,server,port,topic,partitions):

        # get logger isinstance
        self._logger = logging.getLogger("ONI.INGEST.KAFKA")

        # kafka requirements
        self._server = server
        self._port = port
        self._topic = topic
        self._partitions = partitions

        # crate topic and partitions
        self._create_topic()

    def create_topic(self):

        self._logger.info("Creating topic {0} with {1} parititions".format(self._topic,self._partitions))

    def send_message(self,message,partition=None):

        self._logger.info("Sending message to topic {0}".format(self._topic))

