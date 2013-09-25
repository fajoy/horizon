#!/usr/bin/env python
import boto
import zipfile
import httplib,os,json
import euca2ools.utils
import euca2ools.commands.euca.describeinstances
from boto.s3.key import Key
import json
import multiprocessing
import sys
import subprocess

def now():
    import time,datetime
    return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

def read_file(fn):
    with open(fn,'r') as fd:
        return fd.read()
    return None

def del_obj(key):
    s3 = boto.connect_s3()
    b = s3.get_bucket(bucket)
    b.delete_key(key)

def set_obj(key,value):
    s3 = boto.connect_s3()
    b = s3.get_bucket(bucket)
    k = Key(b)
    k.key = key
    k.set_contents_from_string(value)

def save_obj(key,obj):
    s=json.dumps(obj)
    set_obj(key,s)

def get_obj_list(prefix):
    s3 = boto.connect_s3()
    b=s3.get_bucket(bucket)
    query = b.list()
    if prefix:
        if not prefix.endswith("/"):
            prefix = prefix + "/"
        query = b.list(prefix=prefix, delimiter="/")

    for k in query:
        if isinstance(k,Key):
            yield k

def update_file_meta_data(meta):
    fn='/root/meta_data.json'
    with open(fn,"w") as fd:
        json.dump(meta,fd)

def get_file_meta_data():
    fn='/root/meta_data.json'
    if os.path.exists(fn):
        return json.load(open(fn,'r'))
    return None

def get_openstack_meta_data():
    conn = httplib.HTTPConnection("169.254.169.254")
    conn.request("GET", "/openstack/2012-08-10/meta_data.json")
    response = conn.getresponse()
    return json.load(response)

def get_local_ec2_id():
    ec2_id = read_file("/var/lib/cloud/data/previous-instance-id")[:-1]
    return ec2_id

def get_instances_meta():
    cmd = euca2ools.commands.euca.describeinstances.DescribeInstances()
    _instances = {}
    reservations = cmd.main()
    members=( "id", "image_id", "public_dns_name", "private_dns_name",
        "state", "key_name", "ami_launch_index", "product_codes",
        "instance_type", "launch_time", "placement", "kernel",
        "ramdisk", "xx", "_monitoring", 'ip_address', 'private_ip_address',
        "vpc_id", "subnet_id", "root_device_type", "xx", "xx", "xx", "xx",
        "virtualizationType", "hypervisor", "xx", "_groupnames", "_groupids" )
    for reservation in reservations:
        instances = reservation.instances
        i = dict([(instance.id,
                     dict((attr,getattr(instance,attr,None))
                        for attr in members))
             for instance in instances])
        _instances.update(i)
    return _instances

def get_ec2_meta_data():
    ec2_id = get_local_ec2_id()
    instances = get_instances_meta()
    return instances[ec2_id]

def get_obj(key):
    s3 = boto.connect_s3()
    b = s3.get_bucket(bucket)
    k = b.get_key(key)
    if k:
        return k.get_contents_as_string()
    return None

def load_obj(key):
    s=get_obj(key)
    if s:
        return json.loads(s)
    return None

def strings_format(temp_dict,data):
    re = {}
    for key in temp_dict:
        temp=temp_dict[key]
        re[key] = temp.format(**data)
    return re

def update_obj_meta_data(meta):
    key = ".hadoop/{HADOOP_GROUP_ID}/instance/{uuid}".format(**meta)
    save_obj(key,meta)

def get_obj_meta_data(meta):
    key = ".hadoop/{HADOOP_GROUP_ID}/instance/{uuid}".format(**meta)
    return load_obj(key)


def make_hosts_obj(meta):
    prefix =".hadoop/{HADOOP_GROUP_ID}/instance/".format(**meta)
    _hosts= ""
    for key in get_obj_list(prefix):
        instance_meta=json.loads(key.get_contents_as_string())
        if instance_meta.has_key("private_ip_address") :
            _hosts+=instance_meta["private_ip_address"]+" "+ instance_meta["name"]+"\n"
    
    key = ".hadoop/{HADOOP_GROUP_ID}/hosts".format(**meta)
    hosts = """127.0.0.1 localhost

# The following lines are desirable for IPv6 capable hosts
::1 ip6-localhost ip6-loopback
fe00::0 ip6-localnet
ff00::0 ip6-mcastprefix
ff02::1 ip6-allnodes
ff02::2 ip6-allrouters
ff02::3 ip6-allhosts

{hosts}
""".format(**dict(hosts=_hosts))
    set_obj(key,hosts)
    with open('/etc/hosts','w') as fd:
        fd.write(hosts)

def update_file_from_obj(key,filename):
    s3 = boto.connect_s3()
    b = s3.get_bucket(bucket)
    k = b.get_key(key)
    if not isinstance(k,Key):
        return
    if os.path.exists(filename) and  k.size == os.path.getsize(filename):
        return
    if not os.path.exists(os.path.dirname(filename)):
        os.makedirs(os.path.dirname(filename))

    k.get_contents_to_filename(filename)

def update_obj_from_file(key,filename):
    s3 = boto.connect_s3()
    b = s3.get_bucket(bucket)
    k = b.get_key(key)
    if isinstance(k,Key) and k.size == os.path.getsize(file_name):
        return

    k = Key(b)
    k.key = key
    k.set_contents_from_filename(filename)

def update_obj_from_dir(prefix,dirname):
    files_dict={}
    for directory,dirnames,filenames in os.walk(dirname):
        for filename in filenames:
            relpath = os.path.relpath(directory+"/"+filename,start=dirname)
            key ="%s/%s"%(prefix, relpath)
            files_dict[key]="%s/%s"%(directory,filename)


    s3 = boto.connect_s3()
    b=s3.get_bucket(bucket)
    obj_list = b.list(prefix=prefix, delimiter=None)
    for obj in obj_list:
        if files_dict.has_key(obj.name) \
            and os.path.getsize(files_dict[obj.name])== obj.size:
            del files_dict[obj.name]

    for key in files_dict:
        k=Key(b)
        k.key=key
        filename=files_dict[key]
        k.set_contents_from_filename(filename)

def update_hosts(meta):
    key = ".hadoop/{HADOOP_GROUP_ID}/hosts".format(**meta)
    filename="/etc/hosts"
    update_file_from_obj(key,filename)

def update_hostname(meta):
    conf_template={
"/etc/hostname":
"""{name}""",
}
    conf=strings_format(conf_template,meta)
    for fn in conf:
        with open(fn,'w') as fd:
            fd.write(conf[fn])
            fd.close()
    subprocess.check_call("hostname -F /etc/hostname" ,stdout=sys.stdout,stderr=sys.stderr, shell=True ,env=os.environ)
 
def install_hadoop_conf(meta):
    conf_template={
"/etc/hadoop/conf/core-site.xml":
"""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property><name>fs.default.name</name><value>hdfs://{private_ip_address}:8020</value></property>
  <property><name>heartbeat.recheck.interval</name><value>20</value></property>

  <property><name>fs.s3.impl</name><value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value></property>
  <property><name>fs.s3n.impl</name><value>org.apache.hadoop.fs.s3native.NativeS3FileSystem</value></property>
  <property><name>fs.s3n.awsAccessKeyId</name><value>{EC2_ACCESS_KEY}</value></property>
  <property><name>fs.s3n.awsSecretAccessKey</name><value>{EC2_SECRET_KEY}</value></property>
  <property><name>fs.s3.awsAccessKeyId</name><value>{EC2_ACCESS_KEY}</value></property>
  <property><name>fs.s3.awsSecretAccessKey</name><value>{EC2_SECRET_KEY}</value></property>

  <property><name>io.compression.codecs</name><value>org.apache.hadoop.io.compress.DefaultCodec,org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.SnappyCodec</value></property>

<!--
  <property><name>hadoop.metrics.list</name><value>TotalLoad,CapacityTotalGB,UnderReplicatedBlocks,CapacityRemainingGB,PendingDeletionBlocks,PendingReplicationBlocks,CorruptBlocks,CapacityUsedGB,numLiveDataNodes,numDeadDataNodes,MissingBlocks</value></property>
  <property><name>io.compression.codecs</name><value>org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec,com.hadoop.compression.lzo.LzoCodec,com.hadoop.compression.lzo.LzopCodec,org.apache.hadoop.io.compress.BZip2Codec,org.apache.hadoop.io.compress.SnappyCodec</value></property>
  <property><name>io.compression.codec.lzo.class</name><value>com.hadoop.compression.lzo.LzoCodec</value></property>
  <property><name>fs.s3bfs.impl</name><value>org.apache.hadoop.fs.s3.S3FileSystem</value></property>
  <property><name>fs.s3bfs.awsAccessKeyId</name><value></value></property>
  <property><name>fs.s3bfs.awsSecretAccessKey</name><value></value></property>
-->
</configuration>
""",
"/etc/hadoop/conf/hdfs-site.xml":
"""<?xml version="1.0"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
  <property><name>dfs.datanode.https.address</name><value>0.0.0.0:50475</value></property>
  <property><name>dfs.secondary.http.address</name><value>0.0.0.0:50090</value></property>
  <property><name>dfs.http.address</name><value>0.0.0.0:50070</value></property>
  <property><name>dfs.https.address</name><value>0.0.0.0:50470</value></property>
  <property><name>dfs.datanode.http.address</name>0.0.0.0:50075<value></value></property>
  <property><name>dfs.datanode.address</name><value>0.0.0.0:50010</value></property>

  <property><name>dfs.name.dir</name><value>${{hadoop.tmp.dir}}/dfs/name</value></property>
  <property><name>dfs.data.dir</name><value>${{hadoop.tmp.dir}}/dfs/data</value></property>
  <property><name>dfs.replication</name><value>1</value></property>

  <property><name>dfs.datanode.max.xcievers</name><value>4096</value></property>
  <property><name>dfs.datanode.du.reserved</name><value>536870912</value></property>
  <property><name>dfs.namenode.handler.count</name><value>64</value></property>
  <property><name>io.file.buffer.size</name><value>65536</value></property>
  <property><name>dfs.block.size</name><value>134217728</value></property>

<!--
  <property><name>dfs.datanode.ipc.address</name><value>0.0.0.0:50020</value></property>
-->
</configuration>
""",
"/etc/hadoop/conf/mapred-site.xml":
"""<?xml version="1.0"?>
<configuration>
  <property><name>hadoop.job.history.user.location</name><value></value></property>
  <property><name>tasktracker.http.threads</name><value>20</value></property>

  <property><name>mapred.job.tracker.handler.count</name><value>64</value></property>

  <property><name>mapred.job.tracker</name><value>{private_ip_address}:9001</value></property>
  <property><name>mapred.job.tracker.http.address</name><value>0.0.0.0:50030</value></property>

  <property><name>mapred.task.tracker.http.address</name><value>0.0.0.0:50060</value></property>

  <property><name>mapred.tasktracker.map.tasks.maximum</name><value>{map_count}</value></property>
  <property><name>mapred.tasktracker.reduce.tasks.maximum</name><value>{reduce_count}</value></property>

  <property><name>mapred.reduce.tasks</name><value>7</value></property>

  <property><name>mapred.local.dir</name><value>${{hadoop.tmp.dir}}/mapred/local</value></property>

  <property><name>mapred.max.split.size</name><value>67108864</value></property>
  <property><name>mapred.reduce.tasks.speculative.execution</name><value>true</value></property>

  <property><name>mapred.userlog.retain.hours</name><value>48</value></property>
  <property><name>mapred.job.reuse.jvm.num.tasks</name><value>20</value></property>

  <property><name>mapred.reduce.parallel.copies</name><value>20</value></property>
  <property><name>mapred.reduce.tasksperslot</name><value>1.75</value></property>

  <property><name>io.sort.mb</name><value>200</value></property>
  <property><name>io.sort.factor</name><value>48</value></property>

<!--
  <property><name>mapred.map.output.compression.codec</name><value>org.apache.hadoop.io.compress.SnappyCodec</value></property>
  <property><name>mapred.compress.map.output</name><value>true</value></property>
  <property><name>mapred.output.compression.codec</name><value>org.apache.hadoop.io.compress.GzipCodec</value></property>
  <property><name>mapred.output.direct.NativeS3FileSystem</name><value>true</value></property>
  <property><name>mapred.output.committer.class</name><value>org.apache.hadoop.mapred.DirectFileOutputCommitter</value></property>
-->
</configuration>
"""}

    master_meta=get_obj_meta_data(dict(HADOOP_GROUP_ID=meta["HADOOP_GROUP_ID"],
                                    uuid=meta["HADOOP_MASTER_ID"]))
    master_meta["cpu_count"]=multiprocessing.cpu_count()
    master_meta["map_count"]=multiprocessing.cpu_count()*2
    master_meta["reduce_count"]=multiprocessing.cpu_count()
    conf=strings_format(conf_template,master_meta)
    for fn in conf:
        with open(fn,'w') as fd:
            fd.write(conf[fn])
            fd.close()

def update_crontab(meta):
    crontab = """# /etc/crontab: system-wide crontab
# Unlike any other crontab you don't have to run the `crontab'
# command to install the new version when you edit this file
# and files in /etc/cron.d. These files also have username fields,
# that none of the other crontabs do.

SHELL=/bin/bash
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# m h dom mon dow user  command
17 *    * * *   root    cd / && run-parts --report /etc/cron.hourly
25 6    * * *   root    test -x /usr/sbin/anacron || ( cd / && run-parts --report /etc/cron.daily )
47 6    * * 7   root    test -x /usr/sbin/anacron || ( cd / && run-parts --report /etc/cron.weekly )
52 6    1 * *   root    test -x /usr/sbin/anacron || ( cd / && run-parts --report /etc/cron.monthly )
#

*  *    * * *   root    test -e /root/files/check_hadoop_deamon.py && ( cd /root && source /etc/profile && source /root/.eucarc && python /root/files/check_hadoop_deamon.py)
"""
    if meta["HADOOP_TYPE"]=="master":
        crontab += """
*  *    * * *   root    test -e /root/files/check_hadoop_job.py && ( cd /root && source /etc/profile && python /root/files/check_hadoop_job.py)
"""
    with open('/etc/crontab','w') as fd:
        fd.write(crontab)
        fd.close()

meta = dict(os.environ.__dict__["data"])
tenant_id= meta["OS_TENANT_ID"]
group_id= meta["HADOOP_GROUP_ID"]
bucket = "custom-" + tenant_id
master_name= meta["HADOOP_MASTER_NAME"]
if __name__ == "__main__":
    meta.update(get_openstack_meta_data())
    meta.update(get_obj_meta_data(meta))
    meta.update(get_ec2_meta_data())
    instance_id=meta["uuid"]
    meta["cpu_count"]=multiprocessing.cpu_count()
    meta["map_count"]=multiprocessing.cpu_count()*2
    meta["reduce_count"]=multiprocessing.cpu_count()
    update_obj_meta_data(meta)
    update_file_meta_data(meta)
    update_hostname(meta)
    install_hadoop_conf(meta)
    make_hosts_obj(meta)
    update_crontab(meta)
