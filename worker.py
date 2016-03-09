#!/bin/env python

import argparse
import os
import json
from oni.kerberos import Kerberos
import oni.message_broker as Kafka

script_path = os.path.dirname(os.path.abspath(__file__))
conf_file = "{0}/etc/worker_ingest.json".format(script_path)
worker_conf = json.loads(open (conf_file).read())

def main():

    # input parameters
    parser = argparse.ArgumentParser(description="Worker Ingest Framework")
    parser.add_argument('-t','--type',dest='type',required=True,help='Type of data that will be ingested (e.g. dns, flow)',metavar='')
    parser.add_argument('-i','--id',dest='id',required=True,help='Worker Id, this is needed to sync Kafka and Ingest framework (Partitino Number)',metavar='')
    args = parser.parse_args()

    # start worker based on the type.
    start_worker(args.type,args.id)


def start_worker(type,id):

    if not validate_data_source(type)
        print "The provided data source {0} is not valid".format(type)
        sys.exit(1)

    # validate if kerberos authentication is requiered.
    if os.getenv('KRB_AUTH'):
        kb = Kerberos()
        kb.authenticate()

    # create a worker instance based on the data source type.
    module = __import__("oni.{0}.worker".format(type),fromlist=['Worker'])

    # create message broker consumer instance.
    ip_server = worker_conf['message_broker']['ip_server']
    port_server = worker_conf['message_broker']['port_server']
    topic = worker_conf[type]['topic']
    mb_consumer = Kafka.Consumer(ip_server,port_server,topic,id)


    # start worker.
    db_name = worker_conf['dbname']
    app_path = worker_conf['huser']
    ingest_worker = module.Worker(worker_conf[type],db_name,app_path,mb_consumer)
    ingest_worker.start()

def validate_data_source(type):
    is_valid = True if type in worker_conf else False
    return is is_valid

if __name__=='__main__':
    main()

