#/bin/env python

import time
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from multiprocessing import Process
from oni.utils import Util

class Collector(object):

    def __init__(self,conf,app_path,mb_producer):
        self._initialize_members(conf,app_path,mb_producer)

    def _initialize_members(self,conf,app_path,mb_producer):

        # validate configuration info.
        conf_err_msg = "Please provide a valid '{0}' in the configuration file"
        Util.validate_parameter(conf['collector_path'],conf_err_msg.format("collector_path"))
        Util.validate_parameter(conf['topic'],conf_err_msg.format("topic"))
        Util.validate_parameter(conf['pkt_num'],conf_err_msg.format("pkt_num"))
        Util.validate_parameter(conf['pcap_split_staging'],conf_err_msg.format("pcap_split_staging"))
        Util.validate_parameter(conf['time_to_wait'],conf_err_msg.format("time_to_wait"))

        # set configuration.
        self._collector_path = conf['collector_path']
        self._dsource = 'dns'
        self._hdfs_root_path = "{0}/{1}".format(app_path, self._dsource)
        self._topic = conf['topic']
        self._pkt_num = conf['pkt_num']
        self._pcap_split_staging = conf['pcap_split_staging']
        self._time_to_wait =  conf['time_to_wait']

        # initialize message broker client.
        self._mb_producer = mb_producer

    def start(self):

        # start watchdog
        print "Watching path: {0} to collect files".format(self._collector_path)
        event_handler = new_file(self)
        observer = Observer()
        observer.schedule(event_handler,self._collector_path)
        observer.start()

        try:
            while True:
                time.sleep(1)
            except KeyboardInterrupt:
                observer,stop()
                observer.join()

    def load_new_file(self,file):

        # create new process for the new file.
        print "---------------------------------------------------------------------------"
        print "New File received: {0}".format(file)
        if not  ".current" in file and file.endswith(".pcap"):
            p = Process(target=self._ingest_file, args=(file,self._mb_producer.Partition))
            p.start()
            p.join()

    def _ingest_file(self,file,partition):

        # get file name and date.
        file_name_parts = file.split('/')
        file_name = file_name_parts[len(file_name_parts)-1]

        file_date = file_name.split('.')[0]
        file_hour=file_date[-4:-2]
        file_date_path = file_date[-12:-4]

        # hdfs path with timestamp.
        hdfs_path = "{0}/pcap/{1}/{2}".format(self._hdfs_root_path,file_date_path,file_date_hour)
        print hdfs_path
       # Util.creat_hdfs_folder(hdfs_path)

        # get file size.
        file_size = os.stat(file)
        print file_size

        if file_size.st_size > 1145498644:
            # split file.
            self._split_pcap_file(file_name,file_local_path,hdfs_path)
        else:
            # load file to hdfs
            hdfs_file = "{0}/{1}".format(hdfs_path,file_name)
            print hdfs_file
           # Util.load_to_hdfs(file_name,file,hdfs_path)

           # create event for workiers to process the file.
           print "Sending file to worker number: {0}".format(partition)
           self._mb_producer.create_message(hdfs_file,partition)

           print "File has been successfully moved to: {0}".format(file)

class new_file(FileSystemEventHandler):

    _flow_instance = None
    def __init__(self,dns_class):
        self._dns_instance = flow_class

    def on_moved(self,event):
        if not event.is_directory:
            self._dns_instance.load_new_file(event.dest_path)

    def on_created(self,event):
        if not event.is_directory:
            self._dns_instance.load_new_file(event.src_path)
