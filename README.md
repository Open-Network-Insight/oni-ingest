Open-Network-Insight Ingest Framework
======
Ingest data is captured or transferred into the Hadoop cluster, where they are transformed and loaded into solution data stores

![](https://github.com/Open-Network-Insight/oni-ingest/blob/kafka_support/ingest_diagram.png)

## Getting Started
### Prerequisites
* [oni-setup](https://github.com/Open-Network-Insight/oni-setup)
* [kafka-python](https://github.com/dpkp/kafka-python)
* [oni-nfdump](https://github.com/Open-Network-Insight/oni-nfdump)
* tshark
* [watchdog](http://pythonhosted.org/watchdog/)
* Ingest user with sudo privileges (i.e. oni).This user will execute all the processes in the Ingest Framework also this user needs to have access to hdfs solution path (i.e. /user/oni/).

### Configure Kafka
**Adding Kafka Service:**

Ingest framework needs Kafka to work in real-time streaming. Add Kafka service using Cloudera Manager.

**NOTE:** If you are using a Cloudera Manager version < 5.4.1 you will need to add the kafka parcel manually.

**Create Kafka topic:**

A Kafka topic needs to be created with a specific number of partitions, this partition number depends on how many workers are going to be part of the Ingest Framework (num of partitions = num of workers).

    kafka-topics --zookeeper zookeper_server:port --create --replication-factor 1 --partitions "number of workers" --topic "data type (i.e. flow)"
    
If you want to validate that the topic was created:

    kafka-topics.sh --zookeeper zookeper_server:port --describe --topic "data type"

### Installing and Running
**NOTE:** All the nodes where the Ingest Framework is going to be installed, need to have [HDFS getway](https://hadoop.apache.org/docs/r2.4.1/hadoop-project-dist/hadoop-hdfs/HdfsNfsGateway.html) configured (i.e. Edge Server)

     git clone https://github.com/Open-Network-Insight/oni-ingest.git
   
**Standalone:**

    bash start_standalone_ingest.sh "data type (i.e. flow)" "num of workers (same number than partitions in kafka)"

**Cluster:**

**Running Master:** Master needs to be run in the same server where the collector path is.

    python master.py -t "data type (i.e. flow)" -w "number of workers"
    
**Running Workers:** Worker needs to be executed in a server where processing program is installed (i.e. nfdump), also the worker needs to be identified with a specific id, this id needs to start with 0.

example:

1. worker 1,  id = 0 
2. worker 2 , id = 1

This Id is required to attach the worker with the kafka partition.

    python worker.py -t "data type (i.e. flow)" -i "id of the worker (starts with 0)"
    
    
