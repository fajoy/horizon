from django.template.loader import render_to_string
from django.utils.text import normalize_newlines
from django.conf import settings
from django.utils.translation import ugettext_lazy as _
from horizon import exceptions
from horizon import messages
from openstack_dashboard import api
from swiftclient.client  import ClientException
from contextlib import closing
import os
import re
import uuid
import json
import tempfile
import zipfile
import random
from openstack_dashboard.api.nova import novaclient , Server
from novaclient.v1_1 import client as nova_client


CUSTOM_CONTAINER_NAME = getattr(settings,"CUSTOM_CONTAINER_NAME" , "custom-{user.tenant_id}" )
CUSTOM_HADOOP_IMAGE_LIST = getattr(settings,"CUSTOM_HADOOP_IMAGE_LIST" , [] ) 
CUSTOM_HADOOP_PREFIX =  getattr(settings,"CUSTOM_HADOOP_PREFIX" , ".hadoop/" ) 
CUSTOM_HADOOP_S3_HOST = getattr(settings,"CUSTOM_HADOOP_S3_HOST" , "" )


def generate_uid(topic, size=8):
    characters = '01234567890abcdefghijklmnopqrstuvwxyz'
    choices = [random.choice(characters) for _x in xrange(size)]
    return '%s-%s' % (topic, ''.join(choices))

def generate_reservation_id():
    return generate_uid('r')

def get_image_list(request):
    return CUSTOM_HADOOP_IMAGE_LIST

def get_custom_container_name(request):
    return CUSTOM_CONTAINER_NAME.format(**request.__dict__)

def is_exist_container(request,container_name=None):
    if not container_name:
        container_name = CUSTOM_CONTAINER_NAME.format(**request.__dict__)
    return api.swift.swift_container_exists(request,container_name)

def create_container(request,container_name=None):
    if not container_name:
        container_name = CUSTOM_CONTAINER_NAME.format(**request.__dict__)
    if not api.swift.swift_container_exists(request,container_name):
        api.swift.swift_create_container(request, container_name)
    
def get_obj_list(request,container_name=None, prefix=None, marker=None,limit=None,delimiter='/'):
    if not container_name:
        container_name = CUSTOM_CONTAINER_NAME.format(**request.__dict__)
    limit = limit or 1000
    kwargs = dict(prefix=prefix,
                  marker=marker,
                  limit=limit + 1,
                  delimiter=delimiter,
                  full_listing=True)
    try:
        headers, objects = api.swift.swift_api(request).get_container(container_name,**kwargs)
    except ClientException:
        pass

    for item in objects:
        if item.get("name", None):
            yield item

def get_prefix_list(request,container_name=None, prefix=None, marker=None,limit=None,delimiter='/'):
    if not container_name:
        container_name=CUSTOM_CONTAINER_NAME.format(**request.__dict__)
    limit = limit or 1000
    FOLDER_DELIMITER = '/'
    kwargs = dict(prefix=prefix,
                  marker=marker,
                  limit=limit + 1,
                  delimiter=FOLDER_DELIMITER,
                  full_listing=True)
    try:
        headers, objects = api.swift.swift_api(request).get_container(container_name,**kwargs)
    except ClientException:
        return []

    items=[]
    for item in objects:
        if item.get("subdir", None):
            items.append(item)
    return items

def get_obj(request,key,container_name=None):
    if not container_name:
        container_name=CUSTOM_CONTAINER_NAME.format(**request.__dict__)
        obj = api.swift.swift_get_object(
        request,
        container_name,
        key
        )
    return obj.data

def get_hadoop_obj(request,path):
    key = CUSTOM_HADOOP_PREFIX+"{path}".format(**dict(path=path))
    return get_obj(request,key)

def save_obj(request,key,obj,container_name=None):
    if not container_name:
        container_name=CUSTOM_CONTAINER_NAME.format(**request.__dict__)
    object_name = key
    object_file = obj
    size = len(object_file)
    headers = {}
    api.swift.swift_api(request).put_object(container_name,
                                     object_name,
                                     object_file,
                                     content_length=size,
                                     headers=headers)

def delete_obj(request,key,container_name=None):
    if not container_name:
        container_name=CUSTOM_CONTAINER_NAME.format(**request.__dict__)
    api.swift.swift_api(request).delete_object(container_name,key)
    
def gen_id():
    return str(uuid.uuid1())

def now():
    import time
    import datetime
    return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

def get_group_list(request):
    if not is_exist_container(request):
        return []
    l = get_obj_list(request,prefix=CUSTOM_HADOOP_PREFIX+"meta")
    data = []
    for datum in l:
        m=re.search(r"meta/(?P<name>.*)/?$",datum["name"])
        if m:
            datum.update(m.groupdict())
            datum["hadoop_group_id"]=datum["name"]
            data.append(datum)
    return sorted(data,key=lambda o: o.get("last_modified"),reverse=True)

def get_instance_list(request,group_id):
    l = get_obj_list(request,prefix=CUSTOM_HADOOP_PREFIX+group_id+"/instance/")
    data = []
    for datum in l:
        m=re.search(r"(?P<name>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",datum["name"])
        if m:
            datum.update(m.groupdict())
            datum["uuid"]=datum["name"]
            data.append(datum)
    return sorted(data,key=lambda o: o.get("last_modified"),reverse=True)

def get_job_list(request,group_id):
    l = get_obj_list(request,prefix=CUSTOM_HADOOP_PREFIX+"job/"+group_id+"/")
    data = []
    for datum in l:
        m=re.search(r"(?P<name>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",datum["name"])
        if m:
            datum.update(m.groupdict())
            datum["job_id"]=datum["name"]
            data.append(datum)
    return sorted(data,key=lambda o: o.get("last_modified"),reverse=True)

def save_instance_meta(request,meta):
    create_container(request)
    key = CUSTOM_HADOOP_PREFIX+"{HADOOP_GROUP_ID}/instance/{uuid}".format(**meta)
    save_obj(request,key,json.dumps(meta))

def get_instance_meta(request,group_id,instance_id):
    key = CUSTOM_HADOOP_PREFIX+"{hadoop_group_id}/instance/{instance_id}".format(**dict(hadoop_group_id=group_id,
                                                                    instance_id=instance_id))
    return json.loads(get_obj(request,key))

def save_group_meta(request,meta):
    create_container(request)
    key = CUSTOM_HADOOP_PREFIX+"meta/{hadoop_group_id}".format(**meta)
    save_obj(request,key,json.dumps(meta))

def get_group_meta(request,group_id):
    key = CUSTOM_HADOOP_PREFIX+"meta/{hadoop_group_id}".format(**dict(hadoop_group_id=group_id))
    return json.loads(get_obj(request,key))

def save_job_obj(request,context):
    key = CUSTOM_HADOOP_PREFIX+"job/{hadoop_group_id}/{job_id}".format(**context)
    save_obj(request,key,json.dumps(context,indent=4))
    key = CUSTOM_HADOOP_PREFIX+"job/{hadoop_group_id}/queue/{job_id}".format(**context)
    save_obj(request,key,json.dumps(context,indent=4))

def get_job_obj(request,group_id,job_id):
    key = CUSTOM_HADOOP_PREFIX+"job/{hadoop_group_id}/{job_id}".format(**dict(hadoop_group_id=group_id,job_id=job_id))
    return json.loads(get_obj(request,key))


def make_init_script_object(request,group_id,context):

    try:
        s3_endpoint = api.base.url_for(request,
                                       's3',
                                       endpoint_type='publicURL')
    except exceptions.ServiceCatalogException:
        s3_endpoint = None

    try:
        ec2_endpoint = api.base.url_for(request,
                                        'ec2',
                                        endpoint_type='publicURL')
    except exceptions.ServiceCatalogException:
        ec2_endpoint = None

    context.update({'ec2_endpoint': ec2_endpoint,
                    's3_endpoint': s3_endpoint})

    try:
        credentials = api.nova.get_x509_credentials(request)
        cacert = api.nova.get_x509_root_certificate(request)
        temp_zip = tempfile.NamedTemporaryFile(delete=True)
        with closing(zipfile.ZipFile(temp_zip.name, mode='w')) as archive:
            archive.writestr('pk.pem', credentials.private_key)
            archive.writestr('cert.pem', credentials.data)
            archive.writestr('cacert.pem', cacert.data)
            template = 'project/access_and_security/api_access/ec2rc.sh.template'
            archive.writestr('.eucarc', render_to_string(template, context))

            start_path=os.path.abspath(os.path.join(os.path.dirname(__file__),"../templates/hadoop/cloud-config/"))
            files_path=os.path.abspath(os.path.join(os.path.dirname(__file__),"../templates/hadoop/cloud-config/files"))
            for directory,dirnames,filenames in os.walk(files_path):
                for filename in filenames:
                    filepath=directory+"/"+filename
                    relpath=os.path.relpath(directory+"/"+filename,start=start_path)
                    archive.write(filepath,relpath)
    except:
        exceptions.handle(request,
                          _('Error writing zipfile: %(exc)s'),
                          redirect=request.build_absolute_uri())


    container_name = get_custom_container_name(request)
    create_container(request)
    key = CUSTOM_HADOOP_PREFIX+"{group_id}/init_script.zip".format (**{"group_id":group_id ,})
    object_name = key
    object_file = temp_zip.read()
    size = temp_zip.tell()
    headers = {}
    api.swift.swift_api(request).put_object(container_name,
                                         object_name,
                                         object_file,
                                         content_length=size,
                                         headers=headers)


def create_master(request,context):
        dev_mapping = None
        netids = context.get('network_id', None)
        if netids:
            nics = [{"net-id": netid, "v4-fixed-ip": ""}
                    for netid in netids]
        else:
            nics = None

        hadoop_group_id = generate_reservation_id()
        data = {}
        data.update(context)
        data["tenant_id"]=request.user.tenant_id
        data["hadoop_group_id"]=hadoop_group_id
        data["hadoop_master_id"]=""
        data["hadoop_master_name"]=context['name']+'-master'
        data["s3_host"]=CUSTOM_HADOOP_S3_HOST
        make_init_script_object(request,hadoop_group_id,data)
        template = 'custom/hadoop/cloud-config/init_script.template'
        custom_script = render_to_string(template,data)
        meta={}
        data["reservation_id"]= hadoop_group_id
        try:
            instance = Server(novaclient(request).servers.create(
                                   data["hadoop_master_name"],
                                   data['source_id'],
                                   data['flavor'],
                                   key_name=data['keypair_id'],
                                   userdata= normalize_newlines(custom_script),
                                   security_groups=context['security_group_ids'],
                                   block_device_mapping = dev_mapping,
                                   reservation_id=data["reservation_id"],
                                   meta=meta,
                                   nics=nics,
                                   min_count = 1,
                                   admin_pass=data['admin_pass'],
                                    ), request)
            instance = api.nova.server_get(request, instance.id)
            instance_meta=  dict(((attr,getattr(instance,attr,None))  for attr in instance._attrs))
            instance_meta["HADOOP_GROUP_ID"]=data["hadoop_group_id"]
            instance_meta["HADOOP_GROUP_NAME"]=data['name']
            instance_meta["HADOOP_MASTER_ID"]=instance.id
            instance_meta["HADOOP_TYPE"]="master"
            instance_meta["uuid"]=instance_meta["id"]
            save_instance_meta(request,instance_meta)

            data["hadoop_master_id"] = instance.id
            del  data["flavor"]
            del  data["keypair_id"]
            del  data["confirm_admin_pass"]
            del  data['security_group_ids']
            del  data["admin_pass"]
            save_group_meta(request,data)
        except:
            exceptions.handle(request)
            return None
        return data

def _create_slave(request,data):
    template = 'custom/hadoop/cloud-config/init_script.template'
    custom_script = render_to_string(template,data)

    dev_mapping = None
    netids = data.get('network_id', None)
    if netids:
        nics = [{"net-id": netid, "v4-fixed-ip": ""}
                for netid in netids]
    else:
        nics = None


    meta={}
    instance = Server(novaclient(request).servers.create(
                                   data["name"]+"-slave",
                                   data['source_id'],
                                   data['flavor'],
                                   key_name=data['keypair_id'],
                                   userdata= normalize_newlines(custom_script),
                                   security_groups=data['security_group_ids'],
                                   block_device_mapping = dev_mapping,
                                   reservation_id=data["reservation_id"],
                                   meta=meta,
                                   nics=nics,
                                   min_count = 1,
                                   admin_pass=data['admin_pass'],
                                    ), request)


    api.nova.server_update(request,instance.id,data["name"]+"-"+instance.id)
    instance = api.nova.server_get(request, instance.id)
    instance_meta=  dict(((attr,getattr(instance,attr,None))  for attr in instance._attrs))
    instance_meta["HADOOP_GROUP_ID"]=data["hadoop_group_id"]
    instance_meta["HADOOP_GROUP_NAME"]=data['name']
    instance_meta["HADOOP_MASTER_ID"]=data["hadoop_master_id"]
    instance_meta["HADOOP_TYPE"]="slave"
    instance_meta["uuid"]=instance_meta["id"]
    save_instance_meta(request,instance_meta)
    return instance

def create_slave(request,group_id,context):
    data = get_group_meta(request,group_id)
    dev_mapping = None
    data.update(context)
    try:
        for i in xrange(data["count"]):
            instance=_create_slave(request,data)
    except:
        exceptions.handle(request)
        return None
    return data

def delete_job(request,group_id,job_id):
    prefix= CUSTOM_HADOOP_PREFIX + 'job/'+group_id+'/'+job_id
    for obj in get_obj_list(request,prefix=prefix,delimiter=None):
        delete_obj(request,obj["name"])

def terminate_group(request,group_id):
    instances,_more = api.nova.server_list(request)
    nova_list = dict((instance.id,dict(((attr,getattr(instance,attr,None)))  for attr in instance._attrs))for instance in instances)
    nova_ids= set(nova_list.keys())
    meta_list = get_instance_list(request,group_id)
    meta_ids= set( meta["uuid"] for meta in meta_list)
    live_ids= meta_ids & nova_ids
    for id in live_ids:
        api.nova.server_delete(request,id)

def delete_group(request,group_id):
    instances,_more = api.nova.server_list(request)
    nova_list = dict((instance.id,dict(((attr,getattr(instance,attr,None)))  for attr in instance._attrs))for instance in instances)
    nova_ids= set(nova_list.keys())
    meta_list = get_instance_list(request,group_id)
    meta_ids= set( meta["uuid"] for meta in meta_list)
    live_ids= meta_ids & nova_ids
    for id in live_ids:
        api.nova.server_delete(request,id)

    prefix= CUSTOM_HADOOP_PREFIX + 'job/'+group_id
    for obj in get_obj_list(request,prefix=prefix,delimiter=None):
        delete_obj(request,obj["name"])

    prefix= CUSTOM_HADOOP_PREFIX + 'meta/'+group_id
    for obj in get_obj_list(request,prefix=prefix,delimiter=None):
        delete_obj(request,obj["name"])

    prefix= CUSTOM_HADOOP_PREFIX + group_id
    for obj in get_obj_list(request,prefix=prefix,delimiter=None):
        delete_obj(request,obj["name"])

def create_script(request,context):
    new_id = str(uuid.uuid1())
    first_line=context["script"].split("\n")[0]
    m=re.match(r"^#!([\w/\s]+)",first_line)
    if not m:
        create_bash(request,context)
        return context
    path=m.group(1)
    script_type=path.replace("/"," ").split(" ")[-1]
    context.update(
                    {
                     "job_id":new_id 
                    ,"job_type":"script"
                    ,"script_type":script_type
                    }
                  )
    save_job_obj(request,context)
    return context

def create_bash(request,context):
    new_id = str(uuid.uuid1())
    context.update({"job_id":new_id 
                    ,"job_type":"bash"})
    save_job_obj(request,context)
    return context

def create_jar(request,context):
    new_id = str(uuid.uuid1())
    context.update({"job_id":new_id 
                    ,"job_type":"jar"})
    save_job_obj(request,context)
    return context

def create_streaming(request,context):
    new_id = str(uuid.uuid1())
    context.update({"job_id":new_id 
                    ,"job_type":"streaming"})
    save_job_obj(request,context)
    return context
