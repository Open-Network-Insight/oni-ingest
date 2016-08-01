#!/bin/env python

import argparse
import os
import json
import sys

from oni.utils import Util
from oni.kerberos import Kerberos
from oni.kafka import Kafka

# get master configuration.
script_path = os.path.dirname(os.path.abspath(__file__))
conf_file = "{0}/ingest_conf.json".format(script_path)
master_conf = json.loads(open (conf_file).read())

def main():

    # input Parameters
    parser = argparse.ArgumentParser(description="Master Collector Ingest Daemon")
    parser.add_argument('-t','--type',dest='type',required=True,help='Type of data that will be ingested (e.g. dns, flow, proxy)',metavar='')
    parser.add_argument('-w','--num-of-workers',dest='num_workers',required=True,help='Number of workers for the ingest process',metavar='')
    args = parser.parse_args()

    # start collector based on data source type.
    start_collector(args.type,args.num_workers)

def start_collector(type,num_workers):

    # create logger.
    logger = Util.get_logger("ONI.INGEST")

    if not validate_data_source(type):
        logger.error("{0} is not configured.".format(type));sys.exit(1)

    # validate if kerberos authentication is required.
    if os.getenv('KRB_AUTH'):
        kb = Kerberos()
        kb.authenticate()

    # create a collector instance based on data source type.
    logger.info("Starting {0} ingest".format(type))
    module = __import__("pipelines.{0}.collector".format(type),fromlist=['Collector'])

    # create the kafka instance
    logger.info("Creating kafka instance.")
    server = master_conf['kafka_server']['server']
    port = master_conf['kafka_server']['port']
    kafka = Kafka(server,port,type,num_workers)

    # create create kafka topic and partitions.
    kafka.create_topic()

    # start collector.
    ingest_collector = module.Collector(master_conf['hdfs_app_path'],kafka)
    ingest_collector.start()

def validate_data_source(type):

    # type
    dirs = os.walk("{0}/pipelines/".format(script_path)).next()[1]
    is_type_ok = True if type in dirs else False
    return is_type_ok

if __name__=='__main__':
    main()
