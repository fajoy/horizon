#!/usr/bin/env python
import init_script as api
import subprocess
import os
import sys
from multiprocessing import Process
import multiprocessing
import logging

logging.basicConfig(stream=sys.stdout,level=logging.INFO,format="%(asctime)s: %(levelname)s: %(message)s")
log = logging.getLogger(__name__)

def run_master_deamon(meta):
    if not os.path.exists("/var/lib/hadoop-hdfs/cache/hdfs"):
        subprocess.check_call(
        "test -d /var/lib/hadoop-hdfs/cache/hdfs || mkdir -p /var/lib/hadoop-hdfs/cache/hdfs ; chown hdfs:hdfs /var/lib/hadoop-hdfs/cache/hdfs"
        ,stdout=sys.stdout,stderr=sys.stderr, shell=True ,env=os.environ)
        subprocess.check_call("su - hdfs -c '/usr/bin/hadoop namenode -format'",stdout=sys.stdout,stderr=sys.stderr, shell=True ,env=os.environ)
    jps = subprocess.check_output("jps" ,env=os.environ)
    if  jps.find("NameNode")<0:
        log.info("Starting NameNode")
        fd =  open("/root/log/namenode.log","a+")
        subprocess.check_call("su - hdfs hadoop namenode &",stdout=fd,stderr=fd, shell=True )

    if  jps.find("JobTracker")<0:
        log.info("Starting JobTracker")
        fd =  open("/root/log/jobtracker.log","a+")
        subprocess.check_call("su - mapred hadoop jobtracker &",stdout=fd,stderr=fd, shell=True )

def run_slave_deamon(meta):
    hosts = open("/etc/hosts",'r').read()
    master_name=os.environ["HADOOP_MASTER_NAME"]
    if hosts.find(master_name)<0:
        exit(0)


    jps = subprocess.check_output("jps" ,env=os.environ)
    if  jps.find("DataNode")<0:
        if not os.path.exists("/var/lib/hadoop-hdfs/cache/hdfs"):
            subprocess.check_call(
            "test -d /var/lib/hadoop-hdfs/cache/hdfs || mkdir -p /var/lib/hadoop-hdfs/cache/hdfs ; chown hdfs:hdfs /var/lib/hadoop-hdfs/cache/hdfs"
            ,stdout=sys.stdout,stderr=sys.stderr, shell=True ,env=os.environ)
        log.info("Starting DataNode")
        fd =  open("/root/log/datanode.log","a+")
        subprocess.check_call("su - hdfs hadoop datanode &",stdout=fd,stderr=fd, shell=True )

    if  jps.find("TaskTracker")<0:
        log.info("Starting TaskTracker")
        fd =  open("/root/log/tasktracker.log","a+")
        subprocess.check_call("su - mapred hadoop tasktracker &",stdout=fd,stderr=fd, shell=True )

def run_hadoop_deamon(meta):

    if not os.path.exists("/root/log"):
        os.makedirs("/root/log")

    if meta["HADOOP_TYPE"]=="master":
        run_master_deamon(meta)
    if meta["HADOOP_TYPE"]=="slave":
        run_slave_deamon(meta)
    
if __name__ == "__main__":
    meta = api.get_file_meta_data()
    if not meta:
        exit(0)

    jps = subprocess.check_output("jps" ,env=os.environ)
    if  jps.find("Child")>0:
        exit(0)

    api.update_hosts(meta)
    run_hadoop_deamon(meta)
