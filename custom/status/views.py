from django.utils.translation import ugettext_lazy as _
from horizon import exceptions
from openstack_dashboard import api


from django.http import HttpResponse

from openstack_dashboard import api
from horizon import tables

from horizon import messages
from horizon import exceptions


from horizon import forms
from django.core.urlresolvers import reverse

import uuid
import json

import logging
log = logging.getLogger("Custom")



from horizon import conf
def now():
    import time,datetime
    return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')


def getCustomContainerName(request):
    return "custom_"+request.user.tenant_id

def createContainer(request,container_name):
    containers, _more = api.swift.swift_get_containers(request,marker=None)
    if not api.swift.swift_container_exists(request,container_name):
        api.swift.swift_create_container(request, container_name)

def getObj(request,container_name,key):
    obj = api.swift.swift_get_object(
        request,
        container_name,
        key
        )
    return obj.data

def getObjHeader(request,container_name,object_name):
    try:
        return api.swift.swift_api(request).head_object(container_name, object_name)
    except swiftclient.client.ClientException:
        return None



def setObj(request,container_name,key,obj,headers = {}):
        object_name = key
        object_file=obj
        size = len(object_file)
        api.swift.swift_api(request).put_object(container_name,
                                         object_name,
                                         object_file,
                                         content_length=size,
                                         headers=headers)

def delObj(request,container_name,key):
    api.swift.swift_delete_object(request,container_name,key)



def getObjList(request, container_name, prefix=None, marker=None,
                      limit=None):
    limit = limit or 1000
    FOLDER_DELIMITER = '/'
    kwargs = dict(prefix=prefix,
                  marker=marker,
                  limit=limit + 1,
                  delimiter=FOLDER_DELIMITER,
                  full_listing=True)
    from swiftclient.client  import ClientException
    try:
        headers, objects = api.swift.swift_api(request).get_container(container_name,**kwargs)
    except ClientException:
        return []

    return objects



CUSTOM_HOSTS_PREFIX= ".status/hosts/"
CUSTOM_EC2RC_KEY= ".ec2rc/ec2rc.zip"
import json
def saveHost(request,id,datum):
    key=CUSTOM_HOSTS_PREFIX+id
    value=json.dumps(datum)
    container_name=getCustomContainerName(request)
    createContainer(request,container_name)
    setObj(request,container_name,key,value)


def getHost(request,id):
    key=CUSTOM_HOSTS_PREFIX+id
    container_name=getCustomContainerName(request)
    return getObj(request,container_name,key)


def delHost(request,id):
    key=CUSTOM_HOSTS_PREFIX+id
    container_name=getCustomContainerName(request)
    delObj(request,container_name,key)

def getHostList(request):
    data=[]
    container_name=getCustomContainerName(request)
    items = getObjList(request,container_name,prefix=CUSTOM_HOSTS_PREFIX)
    try:
        instances, _more = api.nova.server_list(request)
    except:
        _more = False
        instances = []
    instance_servers = dict([(i.id,i)  for i in instances])
    objs=dict([(i["name"][len(CUSTOM_HOSTS_PREFIX):],i) for i in items])
    from sets import Set
    instances_ids=Set(instance_servers.keys())
    objs_ids=Set(objs.keys())
    no_objs_ids=instances_ids - objs_ids
    for id in no_objs_ids:
        server=instance_servers[id]
        datum={"uuid":server.id,
                "name": server.name,
                "update_time": now()
                }
        saveHost(request,server.id,datum)

    no_instances_ids= objs_ids - instances_ids
    for id in no_instances_ids:
        delHost(request,id)
    

    for i in instances:
        id = i.id
        datum={"uuid":id ,}
        data.append(datum)
                        
    return data

from openstack_dashboard.dashboards.project.access_and_security.api_access.views import download_ec2_bundle
def save_ec2_bundle(request):
    ec2rc_zip=download_ec2_bundle(request).content
    container_name=getCustomContainerName(request)
    key=CUSTOM_EC2RC_KEY
    value=ec2rc_zip
    setObj(request,container_name,key,value)


class LaunchLink(tables.LinkAction):
    name = "create"
    verbose_name = _("Launch Instance")
    url = "horizon:custom:status:launch"
    classes = ("ajax-modal", "btn-create")
 
    
class TerminateInstance(tables.BatchAction):
    name = "terminate"
    action_present = _("Terminate")
    action_past = _("Scheduled termination of")
    data_type_singular = _("Instance")
    data_type_plural = _("Instances")
    classes = ('btn-danger', 'btn-terminate')

    def allowed(self, request, instance=None):
        return True

    def action(self, request, obj_id):
        api.nova.server_delete(request, obj_id)
        delHost(request,obj_id)



class UpdateRow(tables.Row):
    ajax = True

    def get_data(self, request, instance_id):
        datum =json.loads( getHost(request,instance_id))
        return datum



from openstack_dashboard.dashboards.project.instances.tables import AssociateIP,SimpleDisassociateIP
class StatusTable(tables.DataTable):
    def getName(datum):
        return datum.get("name","unknow")
    
    def getState(datum):
        return datum.get("state","unknow")
    
    def get_update_time(datum):
        return datum.get("update_time","unknow")
    
    def getEc2_id(datum):
        return datum.get("id","unknow")
    
    def getAddress(datum):
        ip = datum.get("ip_address","unknow")
        _ip = datum.get("private_ip_address","unknow")
        if ip ==_ip:
            return ip
        else:
            return "%s , %s"%(_ip,ip)



    STATE_CHOICES = (
        ("running", True),
    )
    name = tables.Column(getName,
                         link=("horizon:custom:status:detail"),
                         verbose_name=_("Name"))

    ec2_id = tables.Column(getEc2_id,
                         link=("horizon:custom:status:detail"),
                         verbose_name=_("EC2"))

    state = tables.Column(getState ,
                        verbose_name="State",
                         status=False,
                         status_choices=STATE_CHOICES,
                )
    address = tables.Column(getAddress ,
                        verbose_name="Address",
                )

    update_time = tables.Column(get_update_time,
                        verbose_name="Update" ,
                    )
    def get_object_id(self, datum):
        return datum["uuid"]

    def get_object_display(self, datum):
        return  datum.get("name","unknow")

    class Meta:
        name = "Status"
        status_columns = ["state" , ]
        table_actions = (LaunchLink , TerminateInstance )
        row_actions = (TerminateInstance , AssociateIP ,SimpleDisassociateIP)
        row_class = UpdateRow



from horizon import workflows
from ..config.views import getScriptList,getTemplateScriptRender

class CustomizeAction(workflows.Action):
    customization_script_name = forms.ChoiceField(label=_("Customization Script"),
                                             required=False,
                                           help_text=_("A script or set of "
                                                       "commands to be "
                                                       "executed after the "
                                                       "instance has been "
                                                       "built (max 16kb)."))

    def populate_customization_script_name_choices(self, request, context):
        
        choices = [(datum["name"],datum["name"]) 
                   for datum in getScriptList(request)
                   if (datum["bytes"]>0) ]
        if choices:
            choices.insert(0, ("", _("Select custom script")))
        else:
            choices.insert(0, ("", _("No custom script available.")))
        return choices


    class Meta:
        name = _("Post-Creation")
        help_text_template = ("project/instances/"
                              "_launch_customize_help.html")

class PostCreationStep(workflows.Step):
    action_class = CustomizeAction
    contributes = ("customization_script",)

    def contribute(self, data, context):
        request = self.workflow.request
        name = data.get("customization_script_name", "")
        if name:
            script = getTemplateScriptRender(request,name)
            context['customization_script'] = script
        return context

from openstack_dashboard.dashboards.project.instances.workflows import LaunchInstance, SelectProjectUser,SetInstanceDetails, SetAccessControls,   SetNetwork, VolumeOptions
from django.views.decorators.debug import sensitive_variables
from django.utils.text import normalize_newlines
class CustomLaunchInstance(LaunchInstance):
    name = _("Launch Instance")
    finalize_button_name = _("Launch")
    success_message = _('Launched %(count)s named "%(name)s".')
    failure_message = _('Unable to launch %(count)s named "%(name)s".')
    success_url = "horizon:custom:status:index"
    default_steps = (SelectProjectUser,
                     SetInstanceDetails,
                     SetAccessControls,
                     SetNetwork,
                     VolumeOptions,
                     PostCreationStep)



class LaunchInstanceView(workflows.WorkflowView):
    workflow_class = CustomLaunchInstance

    def get_initial(self):
        initial = super(LaunchInstanceView, self).get_initial()
        initial['project_id'] = self.request.user.tenant_id
        initial['user_id'] = self.request.user.id
        return initial



from horizon import tabs

class HostTab(tabs.TableTab):
    table_classes = ( StatusTable , )
    template_name = "horizon/common/_detail_table.html"
    name = _("Host")
    slug = "host_tab"
    def get_Status_data(self):
        request = self.request
        data = []
        data = getHostList(request)
        return data

class StatusTabs(tabs.TabGroup):
    slug = "status_tabs"
    tabs = (HostTab , )
    sticky = True

class IndexView(tabs.TabbedTableView):
    tab_group_class = StatusTabs
    template_name = 'custom/status/tabs.html'
