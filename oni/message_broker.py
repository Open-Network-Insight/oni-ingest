#!/bin/env python

from kafka import KafkaProducer
from kafka import KafkaConsumer
from kafka.partitioner.roundrobin import RoundRobinPartitioner
from kafka.common import TopicPartition

class Producer(object):

    def __init__(self,server,port,topic,num_partitions):

        self._server = server
        self._port = port
        self._topic = topic
        self._partitions = []
        self._partitioner = None

        # Create partitions for the workers.
        for p in range(int(num_partitions)):
            self._partitions.append(TopicPartition(self._topic,p))

        self._partitioner = RoundRobinPartitioner(self._partitions)

    def create_message(self,message,partition=None):

        self._producer = KafkaProducer(bootstrap_servers=['{0}:{1}'.format(self._server,self._port)])
        future = self._producer.send(self._topic,message,partition=partition)
        self._producer.flush()
        self._producer.close()

    @property
    def Server(self):
        return self._server

    @property
    def Port(self):
        return self._port

    @property
    def Partition(self):
        return self._partitioner.partition(self._topic).partition


class Consumer(object):

    def __init__(self,server,port,topic,id):

        self._server = server
        self._port = port
        self._topic = topic
        self._id = id

    def start(self):

        consumer =  KafkaConsumer(bootstrap_servers=['{0}:{1}'.format(self._server,self._port)],group_id=self._topic)
        partition = [TopicPartition(self._topic,int(self._id))]
        consumer.assign(partitions=partition)
        consumer.poll()
        return consumer

    @property
    def Server(self):
        return self._server

    @property
    def Port(self):
        return self._port

