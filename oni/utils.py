
import sys
import subprocess
import logging

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
    def get_logger(cls,logger_name,create_file=False):
    		

		# create logger for prd_ci
		log = logging.getLogger(logger_name)
		log.setLevel(level=logging.INFO)
		
		# create formatter and add it to the handlers
		formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
		
		if create_file:
				# create file handler for logger.
				fh = logging.FileHandler('oni.log')
				fh.setLevel(level=logging.DEBUG)
				fh.setFormatter(formatter)
		# reate console handler for logger.
		ch = logging.StreamHandler()
		ch.setLevel(level=logging.DEBUG)
		ch.setFormatter(formatter)

		# add handlers to logger.
		if create_file:
			log.addHandler(fh)

		log.addHandler(ch)
		return  log

