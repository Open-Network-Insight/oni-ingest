#!/bin/env python

import sys
from oni.utils import Util
import datetime
from multiprocessing import Process
import subprocess

class Worker(object):

    def __init__(self,worker_conf,db_name,app_path,mb_consumer):
        self._initialize_members(worker_conf,db_name,app_path,mb_consumer)

    def _initialize_members(self,worker_conf,db_name,app_path,mb_consumer):

        # validate parameters
        conf_err_msg = "Please provide a valid '{0}' in the configuration file"
        Util.validate_parameter(db_name,conf_err_msg.format("db_name"))
        Util.validate_parameter(app_path,conf_err_msg.format("app_path"))
        Util.validate_parameter(worker_conf['topic'],conf_err_msg.format("topic"))
        Util.validate_parameter(worker_conf['local_staging'],conf_err_msg.format("local_staging"))

        self._db_name = db_name
        self._app_path = app_path
        self._topic = worker_conf['topic']
        self._process_opt = worker_conf['process_opt']
        self._local_staging = worker_conf['local_staging']
        self._mb_consumer = mb_consumer

    def start(self):

        print "Listening server {0}, topic:{1}".format(self._mb_consumer.Server,self._topic)
        for message in self._mb_consumer.start():
            self._new_file(message.value)

    def _new_file(self,file):

        print "---------------------------------------------------------------------------"
        print " [x] Received {0} DATE:{1}".format(file,datetime.datetime.now())
        p = Process(target=self._process_new_file, args=(file,))
        p.start()
        p.join()

    def _process_new_file(self,file):

        # get file from hdfs
        get_file_cmd = "hadoop fs -get {0} {1}.".format(file,self._local_staging)
        print "Getting file from hdfs: {0}".format(get_file_cmd)
        subprocess.call(get_file_cmd,shell=True)

        # get file name and date
        file_name_parts = file.split('/')
        file_name = file_name_parts[len(file_name_parts)-1]

        flow_date = file_name.split('.')[1]
        flow_year = flow_date[0:4]
        flow_month = flow_date[4:6]
        flow_day = flow_date[6:8]
        flow_hour = flow_date[8:10]


        # build process cmd.
        process_cmd = "nfdump_split_date -o csv -r {0}{1} {2} > {0}{1}.csv".format(self._local_staging,file_name,self._process_opt)
        print "Processing file: {0}".format(process_cmd)
        subprocess.call(process_cmd,shell=True)

        # create hdfs staging.
        hdfs_path = "{0}/flow".format(self._app_path)
        staging_timestamp = datetime.datetime.now().strftime('%M%S%f')[:-4]
        hdfs_staging_path =  "{0}/stage/{1}".format(hdfs_path,staging_timestamp)
        create_staging_cmd = "hadoop fs -mkdir -p {0}".format(hdfs_staging_path)
        print "Creating staging: {0}".format(create_staging_cmd)
        subprocess.call(create_staging_cmd,shell=True)

        # move to stage.
        mv_to_staging ="hadoop fs -moveFromLocal {0}{1}.csv {2}/.".format(self._local_staging,file_name,hdfs_staging_path)
        print "Moving data to staging: {0}".format(mv_to_staging)
        subprocess.call(mv_to_staging,shell=True)

        #load to avro
        load_to_avro_cmd = "hive -hiveconf dbname={0} -hiveconf y={1} -hiveconf m={2} -hiveconf d={3} -hiveconf h={4} -hiveconf data_location='{5}' -f oni/flow/load_flow_avro_parquet.hql".format(self._db_name,flow_year,flow_month,flow_day,flow_hour,hdfs_staging_path)

        print "Loading data to hive: {0}".format(load_to_avro_cmd)
        subprocess.call(load_to_avro_cmd,shell=True)

        # remove from hdfs staging
        rm_hdfs_staging_cmd = "hadoop fs -rm -R -skipTrash {0}".format(hdfs_staging_path)
        print "Removing staging path: {0}".format(rm_hdfs_staging_cmd)
        subprocess.call(rm_hdfs_staging_cmd,shell=True)

        # remove from local staging.
        rm_local_staging = "rm {0}{1}".format(self._local_staging,file_name)
        print "Removing files from local staging: {0}".format(rm_local_staging)
        subprocess.call(rm_local_staging,shell=True)

        print "File {0} was successfully processed.".format(file_name)


