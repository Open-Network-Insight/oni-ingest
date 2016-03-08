
import sys
import subprocess
import oni.message_broker as MB

class Util(object):

    @classmethod
    def validate_parameter(cls,parameter,message):
        if parameter == None or parameter == "":
            print message
            sys.exit(1)

    @classmethod
    def creat_hdfs_folder(cls,hdfs_path):
        hadoop_create_folder="hadoop fs -mkdir -p {0}".format(hdfs_path)
        print "oni.Utils: Creating hdfs folder: {0}".format(hadoop_create_folder)
        subprocess.call(hadoop_create_folder,shell=True)

    @classmethod
    def load_to_hdfs(cls,file_local_path,file_hdfs_path):

        # move file to hdfs.
        load_to_hadoop_script = "hadoop fs -moveFromLocal {0} {1}".format(file_local_path,file_hdfs_path)

        # load_to_hadoop_script = "hadoop fs -put {0} {1}".format(file_local_path,hadoop_pcap_file)
        print "oni.Utils: Loading file to hdfs: {0}".format(load_to_hadoop_script)
        subprocess.call(load_to_hadoop_script,shell=True)

    @classmethod
    def new_message_broker_producer(cls,ip_server,port_server):
        return MB.Producer(ip_server,port_server)

    @classmethod
    def new_message_broker_consumer(cls,ip_server,port_server,topic):
        return MB.Consumer(ip_server,port_server,topic)

