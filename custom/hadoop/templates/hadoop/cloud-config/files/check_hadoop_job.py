#!/usr/bin/env python
import init_script as api
import subprocess
import os
import sys
from boto.s3.key import Key
import re
import json
from time import sleep

def handle_script(obj):
    dir_path = "/root/job/{id}/log".format(**obj)
    os.makedirs(dir_path)
    dir_path = "/root/job/{id}/work".format(**obj)
    os.makedirs(dir_path)
    os.chdir(dir_path)
    with open("/root/job/{id}/obj".format(**obj),"w") as fd:
        fd.write(json.dumps(obj))
    script_path= "/root/job/{id}/run.sh".format(**obj)
    with open(script_path,'a+') as sh:
        cmd = """{script}""".format(**obj)
        sh.write(cmd)
    stdout=open("/root/job/{id}/log/stdout".format(**obj),"a+")
    stderr=open("/root/job/{id}/log/stderr".format(**obj),"a+")
    obj["script_path"]=script_path
    p=subprocess.Popen("{script_type} {script_path}".format(**obj),stdout=stdout,stderr=stderr, shell=True ,env=os.environ)
    log_prefix=".hadoop/job/{group_id}/{id}/log".format(**obj)
    log_dir = "/root/job/{id}/log".format(**obj)
    log_update_sec = int(obj.get("log_update_sec",3))
    if log_update_sec > 0:
        while p.poll()==None: #subprocess is running.
            sleep(log_update_sec)
            api.update_obj_from_dir(log_prefix,log_dir)
    p.wait()
    api.update_obj_from_dir(log_prefix,log_dir)

def handle_bash(obj):
    dir_path = "/root/job/{id}/log".format(**obj)
    os.makedirs(dir_path)
    dir_path = "/root/job/{id}/work".format(**obj)
    os.makedirs(dir_path)
    os.chdir(dir_path)
    with open("/root/job/{id}/obj".format(**obj),"w") as fd:
        fd.write(json.dumps(obj))
    script_path= "/root/job/{id}/run.sh".format(**obj)
    with open(script_path,'a+') as sh:
        cmd = """#!/bin/bash
{script}
""".format(**obj)
        sh.write(cmd)
    stdout=open("/root/job/{id}/log/stdout".format(**obj),"a+")
    stderr=open("/root/job/{id}/log/stderr".format(**obj),"a+")
    p=subprocess.Popen("bash "+script_path,stdout=stdout,stderr=stderr, shell=True ,env=os.environ)
    log_prefix=".hadoop/job/{group_id}/{id}/log".format(**obj)
    log_dir = "/root/job/{id}/log".format(**obj)
    log_update_sec = int(obj.get("log_update_sec",3))
    if log_update_sec > 0:
        while p.poll()==None: #subprocess is running.
            sleep(log_update_sec)
            api.update_obj_from_dir(log_prefix,log_dir)
    p.wait()
    api.update_obj_from_dir(log_prefix,log_dir)

def handle_jar(obj):
    dir_path = "/root/job/{id}/log".format(**obj)
    os.makedirs(dir_path)
    dir_path = "/root/job/{id}/work".format(**obj)
    os.makedirs(dir_path)
    os.chdir(dir_path)
    with open("/root/job/{id}/obj".format(**obj),"w") as fd:
        fd.write(json.dumps(obj))
    jar_location = obj["jar_location"]
    obj["jar_filename"] = os.path.basename(jar_location)
    obj["jar_filepath"] = "/root/job/{id}/work/{jar_filename}".format(**obj)
    script_path= "/root/job/{id}/run.sh".format(**obj)
    
    with open(script_path,'a+') as sh:
        cmd = """#!/bin/bash
echo hadoop fs -get s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@{jar_location} {jar_filepath} >&2
hadoop fs -get s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@{jar_location} {jar_filepath} 

echo hadoop jar {jar_filepath} {jar_args} >&2
hadoop jar {jar_filepath} {jar_args} 
""".format(**obj)
        sh.write(cmd)

    stdout=open("/root/job/{id}/log/stdout".format(**obj),"a+")
    stderr=open("/root/job/{id}/log/stderr".format(**obj),"a+")
    p=subprocess.Popen("bash "+script_path,stdout=stdout,stderr=stderr, shell=True ,env=os.environ)
    log_prefix=".hadoop/job/{group_id}/{id}/log".format(**obj)
    log_dir = "/root/job/{id}/log".format(**obj)
    log_update_sec = int(obj.get("log_update_sec",3))
    if log_update_sec > 0:
        while p.poll()==None: #subprocess is running.
            sleep(log_update_sec)
            api.update_obj_from_dir(log_prefix,log_dir)
    p.wait()
    api.update_obj_from_dir(log_prefix,log_dir)


def handle_streaming(obj):
    dir_path = "/root/job/{id}/log".format(**obj)
    os.makedirs(dir_path)
    dir_path = "/root/job/{id}/work".format(**obj)
    os.makedirs(dir_path)
    os.chdir(dir_path)
    with open("/root/job/{id}/obj".format(**obj),"w") as fd:
        fd.write(json.dumps(obj))
    obj["jar_filename"] = "/usr/share/hadoop/contrib/streaming/hadoop-streaming-1.0.2.jar"
    obj["mapper_filename"] = os.path.basename(obj["mapper"])
    obj["reducer_filename"] = os.path.basename(obj["reducer"])
    obj["jar_filepath"] = "/root/job/{id}/work/{jar_filename}".format(**obj)
    
    script_path= "/root/job/{id}/run.sh".format(**obj)
    with open(script_path,'a+') as sh:
        cmd = """#!/bin/bash
echo hadoop fs -get s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@{mapper} ./ >&2
hadoop fs -get s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@{mapper} ./

echo hadoop fs -get s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@{reducer} ./ >&2
hadoop fs -get s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@{reducer} ./


echo hadoop jar {jar_filename} -input s3n://{input_location} -output s3n://{output_location} -mapper {mapper_filename} -reducer {reducer_filename} -file {mapper_filename}  -file {reducer_filename}  {extea_args} >&2
hadoop jar {jar_filename} -input s3n://{input_location} -output s3n://{output_location} -mapper {mapper_filename} -reducer {reducer_filename} -file {mapper_filename}  -file {reducer_filename}  {extea_args} 

echo hadoop job -history  s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@{output_location} >&2
hadoop job -history  s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@{output_location} 
""".format(**obj)
        sh.write(cmd)

    stdout=open("/root/job/{id}/log/stdout".format(**obj),"a+")
    stderr=open("/root/job/{id}/log/stderr".format(**obj),"a+")
    p=subprocess.Popen("bash "+script_path,stdout=stdout,stderr=stderr, shell=True ,env=os.environ)
    log_prefix=".hadoop/job/{group_id}/{id}/log".format(**obj)
    log_dir = "/root/job/{id}/log".format(**obj)
    log_update_sec = int(obj.get("log_update_sec",3))
    if log_update_sec > 0:
        while p.poll()==None: #subprocess is running.
            sleep(log_update_sec)
            api.update_obj_from_dir(log_prefix,log_dir)
    p.wait()
    api.update_obj_from_dir(log_prefix,log_dir)

def handle_job(obj):
    if obj["job_type"]=="script":
        handle_script(obj)
        return

    if obj["job_type"]=="bash":
        handle_bash(obj)
        return

    if obj["job_type"]=="jar":
        handle_jar(obj)
        return

    if obj["job_type"]=="streaming":
        handle_streaming(obj) 
        return

def pop_obj():
    prefix= ".hadoop/job/{group_id}/queue/".format(**{"group_id":api.group_id})
    for obj in  api.get_obj_list(prefix):
        m=re.search(r"(?P<id>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",obj.name)
        if m:
            gdict=m.groupdict()
            gdict["bucket"]=api.bucket
            gdict["group_id"]=api.group_id
            job_obj = api.load_obj(obj.name)
            job_obj.update(gdict)
            api.del_obj(obj.name)
            yield job_obj

def run_job():
    for obj in pop_obj():
        pid = os.fork()
        if pid ==0:
            handle_job(obj)
            exit(0)

def save_pid():
    with open("/var/run/%s.pid"%os.path.basename(__file__),"w") as fd:
        fd.write(str(os.getpid()))
        fd.close()
    
def load_pid():
    if not os.path.exists("/var/run/%s.pid"%os.path.basename(__file__)):
        return None
    with open("/var/run/%s.pid"%os.path.basename(__file__),"r") as fd:
        return int(fd.read())

def is_live(pid):
    try:
        os.kill(pid, 0 )
        return True
    except OSError,e:
        return False

if __name__ == "__main__":
    pid=load_pid()
    if pid !=None and is_live(pid):
        exit(0)
    save_pid()
    meta = api.get_file_meta_data()
    if not meta:
        exit(0)
    if meta["HADOOP_TYPE"]=="master":
        while True :
            run_job()
            sleep(5)
