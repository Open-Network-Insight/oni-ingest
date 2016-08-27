#!/bin/env python

import argparse
import os
import json
import logging
import sys
from oni.utils import Util
from oni.kerberos import Kerberos
from oni.kafka_client import KafkaConsumer

script_path = os.path.dirname(os.path.abspath(__file__))
conf_file = "{0}/ingest_conf.json".format(script_path)
worker_conf = json.loads(open (conf_file).read())

def main():

    # input parameters
    parser = argparse.ArgumentParser(description="Worker Ingest Framework")
    parser.add_argument('-t','--type',dest='type',required=True,help='Type of data that will be ingested (e.g. dns, flow)',metavar='')
    parser.add_argument('-i','--id',dest='id',required=True,help='Worker Id, this is needed to sync Kafka and Ingest framework (Partition Number)',metavar='')
    parser.add_argument('-top','--topic',dest='topic',required=True,help='Topic to read from.',metavar="")
    args = parser.parse_args()

    # start worker based on the type.
    start_worker(args.type,args.topic,args.id)


def start_worker(type,topic,id):

    logger = Util.get_logger("ONI.INGEST.PROXY")

    if not Util.validate_data_source(type):
        logger.error("The provided data source {0} is not valid".format(type));sys.exit(1)

    # validate if kerberos authentication is requiered.
    if os.getenv('KRB_AUTH'):
        kb = Kerberos()
        kb.authenticate()

    # create a worker instance based on the data source type.
    module = __import__("pipelines.{0}.worker".format(type),fromlist=['Worker'])

    # create message broker consumer instance.

    # kafka server info.
    logger.info("Initializing kafka instance")
    k_server = worker_conf["kafka"]['kafka_server']
    k_port = worker_conf["kafka"]['kafka_port']

    # required zookeeper info.
    zk_server = worker_conf["kafka"]['zookeper_server']
    zk_port = worker_conf["kafka"]['zookeper_port']
    topic = topic

    #remove this.
    partition = 0

    # create kafka consumer.
    kafka_consumer = KafkaConsumer(topic,k_server,k_port,zk_server,zk_port,partition)

    # start worker.
    db_name = worker_conf['dbname']
    app_path = worker_conf['hdfs_app_path']
    ingest_worker = module.Worker(db_name,app_path,kafka_consumer)
    ingest_worker.start()

if __name__=='__main__':
    main()

