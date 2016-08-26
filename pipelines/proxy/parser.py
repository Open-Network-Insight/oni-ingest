
from __future__ import print_function

import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils

import re
import shlex

message = ""



def oni_decoder(s):

    """ Decode the unicode as UTF-8 """
    if s is None:
        return None
    return s

def split_log_entry(line):
    
    lex = shlex.shlex(line)
    lex.quotes = '"'
    lex.whitespace_split = True
    lex.commenters = ''
    return list(lex)

def proxy_parser(pl):

    proxy_parsed_data = []
    proxy_log_rows = pl.split("\n")

    for proxy_log in proxy_log_rows:

        if rex_date.match(proxy_log):
        
            # clean up the proxy log register.
            line = proxy_log.strip("\n").strip("\r") 
            line = line.replace("\t", " ").replace("  ", " ")
            proxy_fields = split_log_entry(line) 
	

            # create full URI.
            proxy_uri_path =  proxy_fields[17] if  len(proxy_fields[17]) > 1 else ""
            proxy_uri_qry =  proxy_fields[18] if  len(proxy_fields[18]) > 1 else "" 
            full_uri= "{0}{1}{2}".format(proxy_fields[15],proxy_uri_path,proxy_uri_qry)

            # re-order fields.
            proxy_parsed_data.append([proxy_fields[0],proxy_fields[1],proxy_fields[3],proxy_fields[15],proxy_fields[12],proxy_fields[20],proxy_fields[13],proxy_fields[2],proxy_fields[4],
            proxy_fields[5],proxy_fields[6],proxy_fields[7],proxy_fields[8],proxy_fields[9],proxy_fields[10],proxy_fields[11],proxy_fields[14],proxy_fields[16],proxy_fields[17],proxy_fields[18],
            proxy_fields[19],proxy_fields[21],proxy_fields[22],proxy_fields[23],proxy_fields[24],proxy_fields[25],proxy_fields[26],full_uri])


    return proxy_parsed_data

if __name__ == "__main__":

    rex_date = re.compile("\d{4}-\d{2}-\d{2}")

    if len(sys.argv) != 3:
        print("Usage: kafka_wordcount.py <zk> <topic>", file=sys.stderr)
        exit(-1)
	
    # get zp parameter.
    zkQuorum, topic = sys.argv[1:]

 	# create spark context
    sc = SparkContext(appName="ONI-Ingest-{0}".format(topic))
    ssc = StreamingContext(sc, 1)

    # create stream from kafka
    kvs = KafkaUtils.createStream(ssc, zkQuorum, "spark-streaming-consumer", {topic: 1},keyDecoder=oni_decoder,valueDecoder=oni_decoder)

    # parse proxy log.
    lines = kvs.map(lambda x: proxy_parser(x[1]))

    # send data to hdfs.
    

    lines.pprint()    
    ssc.start()
    ssc.awaitTermination()