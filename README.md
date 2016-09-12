Open-Network-Insight Ingest Framework
======
Ingest data is captured or transferred into the Hadoop cluster, where they are transformed and loaded into solution data stores

![Ingest Framework](docs/ONI_Ingest_Framework1_1.png)

## Getting Started

### Prerequisites
* [oni-setup](https://github.com/Open-Network-Insight/oni-setup)
* [kafka-python](https://github.com/dpkp/kafka-python)
* [oni-nfdump](https://github.com/Open-Network-Insight/oni-nfdump)
* tshark
  * [download](https://www.wireshark.org/download.html)
  * [how to install](https://github.com/Open-Network-Insight/open-network-insight/wiki/Install%20Ingest%20Prerequisites)
* [watchdog](http://pythonhosted.org/watchdog/)
* [spark-streaming-kafka-0-8-assembly_2.11](http://search.maven.org/#search|ga|1|a%3A%22spark-streaming-kafka-0-8-assembly_2.11%22%20AND%20v%3A%222.0.0%22)	 
* Ingest user with sudo privileges (i.e. oni). This user will execute all the processes in the Ingest Framework also this user needs to have access to hdfs solution path (i.e. /user/oni/).

### Configure Kafka
**Adding Kafka Service:**

Ingest framework needs Kafka to work in real-time streaming. Add Kafka service using Cloudera Manager. If you are using a Cloudera Manager version < 5.4.1 you will need to add the kafka parcel manually.

Ingest module uses a default configuration for the message size (999999 bytes), if you modify this size in the ingest configuration file you need to modify the following configuration properties in kafka:

* message.max.bytes
* replica.fetch.max.bytes

### Spark-Streaming Kafka support.
Download [spark-streaming-kafka-0-8-assembly_2.11](http://search.maven.org/#search|ga|1|a%3A%22spark-streaming-kafka-0-8-assembly_2.11%22%20AND%20v%3A%222.0.0%22). This jar adds support for Spark Streaming + Kafka and needs to be downloaded in the following path : **oni-ingest/oni**

### Getting Started

**Required Roles**
The following roles are required in all the nodes where the Ingest Framework will be runing.
* [HDFS gateway (i.e. Edge Server)](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/HdfsNfsGateway.html)
* Kafka Broker

**Get the code:**

     git clone https://github.com/Open-Network-Insight/oni-ingest.git

**Ingest Configuration:**

The file **ingest_conf.json** contains all the required configuration to start the ingest module
*  **dbname:** Name of HIVE database where all the ingested data will be stored in avro-parquet format.
*  **hdfs_app_path:** Application path in HDFS where the pipelines will be stored (i.e /user/_application_user_/). 
*  **kafka:** Kafka and Zookeeper server information required to create/listen topics and partitions.
*  **pipelines:** In this section you can add multiple configurations for either the same pipeline or different pipelines. The configuration name must be lowercase without spaces (i.e. flow_internals).

**Configuration example:**

      "pipelines":{
      
         "flow_internals":{
              "type":"flow",
              "collector_path":"/path_to_flow_collector",
              "local_staging":"/tmp/",
              "process_opt":""
          },
          "flow_externals":{
              "type":"flow",
              "collector_path":"/path_to_flow_collector",
              "local_staging":"/tmp/",
              "process_opt":""
          },
          "dns_server_1":{
              "type":"dns",
              "collector_path":"/path_to_dns_collector",
              "local_staging":"/tmp/",
              "pkt_num":"650000",
              "pcap_split_staging":"/tmp",    
              "process_opt":"-E separator=, -E header=y -E occurrence=f -T fields -e frame.time -e frame.time_epoch -e frame.len -e ip.src -e ip.dst -e dns.resp.name -e dns.resp.type -e dns.resp.class -e dns.flags.rcode -e dns.a 'dns.flags.response == 1'"
          }

**Starting the Ingest**

_Standalone Mode:_

    bash start_standalone_ingest.sh "pipeline_configuration" "num of workers"
    
Following the previous configuration example starting ingest module in a stand alone mode will look like:

    bash start_standalone_ingest.sh flow_internals 4

_Cluster Mode:_

**Running Master:** Master needs to be run in the same server where the collector path is.

    python master_collector.py -t "pipeline_configuration" -w "number of workers"
    
**Running Workers:** Worker needs to be executed in a server where processing program is installed (i.e. nfdump), also the worker needs to be identified with a specific id, this id needs to start with 0.

example:

1. worker_0,  id = 0 
2. worker_1 , id = 1

This "id" is required to attach the worker with the kafka partition.

    python worker.py -t "pipeline_configuration" -i "id of the worker (starts with 0)" --topic "my_topic"
    
    
