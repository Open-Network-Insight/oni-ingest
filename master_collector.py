#!/bin/env python

import argparse
import os
import json
import sys
from oni.utils import Util
from oni.kerberos import Kerberos
from oni.kafka_client import KafkaTopic
import datetime 

# get master configuration.
script_path = os.path.dirname(os.path.abspath(__file__))
conf_file = "{0}/ingest_conf.json".format(script_path)
master_conf = json.loads(open (conf_file).read())

def main():

    # input Parameters
    parser = argparse.ArgumentParser(description="Master Collector Ingest Daemon")
    parser.add_argument('-t','--type',dest='type',required=True,help='Type of data that will be ingested (e.g. dns, flow, proxy)',metavar='')
    parser.add_argument('-w','--workers',dest='workers_num',required=True,help='Number of workers for the ingest process',metavar='')
    args = parser.parse_args()

    # start collector based on data source type.
    start_collector(args.type,args.workers_num)

def start_collector(type,workers_num):

    # generate ingest id
    ingest_id = str(datetime.datetime.time(datetime.datetime.now())).replace(":","_").replace(".","_")   

    # create logger.
    logger = Util.get_logger("ONI.INGEST")

    if not Util.validate_data_source(type):
        logger.error("'{0}' type is not configured.".format(type));
        sys.exit(1)

    # validate if kerberos authentication is required.
    if os.getenv('KRB_AUTH'):
        kb = Kerberos()
        kb.authenticate()
    
    # kafka server info.
    logger.info("Initializing kafka instance")
    k_server = master_conf["kafka"]['kafka_server']
    k_port = master_conf["kafka"]['kafka_port']

    # required zookeeper info.
    zk_server = master_conf["kafka"]['zookeper_server']
    zk_port = master_conf["kafka"]['zookeper_port']

    topic = "{0}_{1}".format(type,ingest_id)    
    kafka = KafkaTopic(topic,k_server,k_port,zk_server,zk_port,workers_num)

    # create a collector instance based on data source type.
    logger.info("Starting {0} ingest instance".format(type))
    module = __import__("pipelines.{0}.collector".format(type),fromlist=['Collector'])

    # start collector.
    ingest_collector = module.Collector(master_conf['hdfs_app_path'],kafka)
    ingest_collector.start()

if __name__=='__main__':
    main()
