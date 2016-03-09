#!/bin/env python

import argparse
import os
import json

from oni.kerberos import Kerberos
import oni.message_broker as Kafka

script_path = os.path.dirname(os.path.abspath(__file__))
conf_file = "{0}/etc/master_ingest.json".format(script_path)
master_conf = json.loads(open (conf_file).read())

def main():

    # input Parameters
    parser = argparse.ArgumentParser(description="Master Ingest Daemon")
    parser.add_argument('-t','--type',dest='type',required=True,help='Type of data that will be ingested (e.g. dns, flow)',metavar='')
    parser.add_argument('-w','--num-of-workers',dest='num_workers',required=True,help='Number of workers for the ingest process',metavar='')
    args = parser.parse_args()

    # start collector based on data source type.
    start_collector(args.type,args.num_workers)

def start_collector(type,num_workers):

    if not validate_data_source(type):
        print "The provided data source type {0} is not a valid".format(type)
        sys.exit(1)

    # validate if kerberos authentication is required.
    if os.getenv('KRB_AUTH'):
        kb = Kerberos()
        kb.authenticate()

    # create a collector instance based on data source type.
    module = __import__("oni.{0}.collector".format(type),fromlist=['Collector'])

    # create the message broker produer instance
    ip_server = master_conf['message_broker']['ip_server']
    port_server = master_conf['message_broker']['port_server']
    mb_producer = Kafka.Producer(ip_server,port_server,type,num_workers)

    # start collector.
    ingest_collector = module.Collector(master_conf[type],master_conf['huser'],mb_producer)
    ingest_collector.start()

def validate_data_source(type):
    is_valid = True if type in master_conf else False
    return is is_valid

if __name__=='__main__':
    main()
