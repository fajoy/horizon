from django.utils.translation import ugettext_lazy as _
from horizon import exceptions
from openstack_dashboard import api
from django.conf import settings


from django.http import HttpResponse

from openstack_dashboard import api
from horizon import tables

from horizon import messages
from horizon import exceptions


from horizon import workflows
from horizon import forms
from django.core.urlresolvers import reverse
from django.core.urlresolvers import resolve

import uuid
import json

import logging
log = logging.getLogger("Custom")


HADOOP_IMAGE_LIST=getattr(settings,"HADOOP_IMAGE_LIST" , [] ) 
CUSTOM_HADOOP_PREFIX= ".hadoop/" 


def getObj(request,container_name,key):
    obj = api.swift.swift_get_object(
        request,
        container_name,
        key
        )
    return obj.data


def setObj(request,container_name,key,obj):
        object_name = key
        object_file = obj
        size = len(object_file)
        headers = {}
        api.swift.swift_api(request).put_object(container_name,
                                         object_name,
                                         object_file,
                                         content_length=size,
                                         headers=headers)

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
        
    items=[]
    for item in objects:
        if item.get("name", None):
            items.append(item)
    return items


def getHadoopGroupList(request):
    container_name=getCustomContainerName(request)
    data = getObjList(request,container_name,prefix=CUSTOM_HADOOP_PREFIX)
    for d in data:
        d.update({"hadoop_group_id":d["name"][-36:]})
    return data
 
def getHadoopInstancesList(request,group_id):
    container_name=getCustomContainerName(request)
    prefix= CUSTOM_HADOOP_PREFIX+group_id+"/hosts/"
    data = getObjList(request,container_name,prefix=prefix)
    for d in data:
        d.update({"uuid":d["name"][-36:]})
    return data
 
       
def genId():
    return str(uuid.uuid1())

def now():
    import time
    import datetime
    return datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

def getCustomContainerName(request):
    return "custom_"+request.user.tenant_id

def createContainer(request,container_name):
    containers, _more = api.swift.swift_get_containers(request,marker=None)
    if not api.swift.swift_container_exists(request,container_name):
        api.swift.swift_create_container(request, container_name)

def setHadoopGroup(request,data):
    container_name = getCustomContainerName(request)
    key = ".hadoop/.%s" %(data["hadoop_group_id"])
    createContainer(request,container_name)
    setObj(request,container_name,key,json.dumps(data))

def setHadoopJob(request,data):
    container_name = getCustomContainerName(request)
    key = ".hadoop/{group_id}/job/{id}".format(**data)
    createContainer(request,container_name)
    setObj(request,container_name,key,json.dumps(data))


def getEc2Keys(request):
    tenant_id = request.user.tenant_id
    all_keys = api.keystone.list_ec2_credentials(request,
                                                     request.user.id)


    try:
        all_keys = api.keystone.list_ec2_credentials(request,
                                                     request.user.id)
        keys = None
        for key in all_keys:
            if key.tenant_id == tenant_id:
                keys = key
        if keys is None:
            keys = api.keystone.create_ec2_credentials(request,
                                                       request.user.id,
                                                       tenant_id)
    except:
        exceptions.handle(request,
                          _('Unable to fetch EC2 credentials.'),
                          redirect=request.build_absolute_uri())

    context = {'ec2_access_key': keys.access,
               'ec2_secret_key': keys.secret,
               }
    return context


from openstack_dashboard.dashboards.project.instances.workflows import SetInstanceDetailsAction,KEYPAIR_IMPORT_URL
from openstack_dashboard.dashboards.project.instances.workflows import LaunchInstance, SetInstanceDetails, SetNetwork, VolumeOptions
from openstack_dashboard.dashboards.project.images_and_snapshots.utils import get_available_images
from horizon.utils import validators
class SetAccessControlsAction(workflows.Action):
    keypair = forms.DynamicChoiceField(label=_("Keypair"),
                                       required=False,
                                       help_text=_("Which keypair to use for "
                                                   "authentication."),
                                       add_item_link=KEYPAIR_IMPORT_URL)
    admin_pass = forms.RegexField(
            label=_("Root Pass"),
            required=False,
            widget=forms.PasswordInput(render_value=False),
            regex=validators.password_validator(),
            error_messages={'invalid': validators.password_validator_msg()})
    confirm_admin_pass = forms.CharField(
            label=_("Confirm Root Pass"),
            required=False,
            widget=forms.PasswordInput(render_value=False))
    groups = forms.MultipleChoiceField(label=_("Security Groups"),
                                       required=True,
                                       initial=["default"],
                                       widget=forms.CheckboxSelectMultiple(),
                                       help_text=_("Launch instance in these "
                                                   "security groups."))

    class Meta:
        name = _("Access & Security")
        help_text = _("Control access to your instance via keypairs, "
                      "security groups, and other mechanisms.")

    def populate_keypair_choices(self, request, context):
        try:
            keypairs = api.nova.keypair_list(request)
            keypair_list = [(kp.name, kp.name) for kp in keypairs]
        except:
            keypair_list = []
            exceptions.handle(request,
                              _('Unable to retrieve keypairs.'))
        if keypair_list:
            if len(keypair_list) == 1:
                self.fields['keypair'].initial = keypair_list[0][0]
            keypair_list.insert(0, ("", _("Select a keypair")))
        else:
            keypair_list = (("", _("No keypairs available.")),)
        return keypair_list

    def populate_groups_choices(self, request, context):
        try:
            groups = api.nova.security_group_list(request)
            security_group_list = [(sg.name, sg.name) for sg in groups]
        except:
            exceptions.handle(request,
                              _('Unable to retrieve list of security groups'))
            security_group_list = []
        return security_group_list

    def clean(self):
        '''Check to make sure password fields match.'''
        cleaned_data = super(SetAccessControlsAction, self).clean()
        if 'admin_pass' in cleaned_data:
            if cleaned_data['admin_pass'] != cleaned_data.get(
                    'confirm_admin_pass', None):
                raise forms.ValidationError(_('Passwords do not match.'))
        return cleaned_data

class SetAccessControls(workflows.Step):
    action_class = SetAccessControlsAction
    contributes = ("keypair_id", "security_group_ids",
            "admin_pass", "confirm_admin_pass")

    def contribute(self, data, context):
        if data:
            post = self.workflow.request.POST
            context['security_group_ids'] = post.getlist("groups")
            context['keypair_id'] = data.get("keypair", "")
            context['admin_pass'] = data.get("admin_pass", "")
            context['confirm_admin_pass'] = data.get("confirm_admin_pass", "")
        return context

#ref openstack_dashboard/dashboards/project/instances/workflows/create_instance.py
from openstack_dashboard.usage import quotas
class SetHadoopGroupDetailsAction(workflows.Action):
    source_id = forms.ChoiceField(label=_("Image"), required=True)
    name = forms.CharField(max_length=80, label=_("Group Name"))
    master_flavor = forms.ChoiceField(label=_("Master Instance Flavor"),
                               help_text=_("Size of image to launch."))

    flavor = forms.ChoiceField(label=_("Core Instance Flavor"),
                               help_text=_("Size of image to launch."))
    count = forms.IntegerField(label=_("Core Instance Count"),
                               min_value=1,
                               initial=1,
                               help_text=_("Number of instances to launch."))

    class Meta:
        name = _("Details")
        help_text_template = ("project/instances/"
                              "_launch_details_help.html")

    def clean(self):
        cleaned_data = super(SetHadoopGroupDetailsAction, self).clean()
        return cleaned_data

    def _init_images_cache(self):
        if not hasattr(self, '_images_cache'):
            self._images_cache = {}

    def populate_source_id_choices(self, request, context):
        self._init_images_cache()
        images = get_available_images(request, context.get('project_id'),
                                      self._images_cache)
        choices = [(image.id, image.name)
                   for image in images
                   if image.properties.get("image_type", '') != "snapshot" 
                      and str(image.id) in HADOOP_IMAGE_LIST ]  
        if choices:
            choices.insert(0, ("", _("Select Image")))
        else:
            choices.insert(0, ("", _("No images available.")))
        return choices

    def populate_flavor_choices(self, request, context):
        try:
            flavors = api.nova.flavor_list(request)
            flavor_list = [(flavor.id, "%s" % flavor.name)
                           for flavor in flavors]
        except:
            flavor_list = []
            exceptions.handle(request,
                              _('Unable to retrieve instance flavors.'))
        return sorted(flavor_list)

    def populate_master_flavor_choices(self, request, context):
        try:
            flavors = api.nova.flavor_list(request)
            flavor_list = [(flavor.id, "%s" % flavor.name)
                           for flavor in flavors]
        except:
            flavor_list = []
            exceptions.handle(request,
                              _('Unable to retrieve instance flavors.'))
        return sorted(flavor_list)

    def get_help_text(self):
        extra = {}
        try:
            extra['usages'] = quotas.tenant_quota_usages(self.request)
            extra['usages_json'] = json.dumps(extra['usages'])
            flavors = json.dumps([f._info for f in
                                       api.nova.flavor_list(self.request)])
            extra['flavors'] = flavors
        except:
            exceptions.handle(self.request,
                              _("Unable to retrieve quota information."))
        return super(SetHadoopGroupDetailsAction, self).get_help_text(extra)



class SetHadoopGroupDetails(workflows.Step):
    action_class = SetHadoopGroupDetailsAction
    contributes = ( "source_id", "name", "count", "flavor","master_flavor")
    class Meta:
        name = _("Details")
        help_text_template = ("project/instances/"
                              "_launch_details_help.html")



from django.template.loader import render_to_string
from django.utils.text import normalize_newlines
class CreateHadoopGroup(workflows.Workflow):
    slug = "create_group"
    name = _("Create Hadoop Group")
    finalize_button_name = _("Create")
    success_message = _('Created Hadoop Group "%s".')
    failure_message = _('Unable to create Hadoop Group "%s".')
    success_url = "horizon:custom:hadoop:index"
    default_steps = (
                     SetHadoopGroupDetails,
                     SetAccessControls,
                     SetNetwork,
                    )


    def format_status_message(self, message):
        name = self.context.get('name', 'unknown instance')
        count = self.context.get('count', 1)
        if int(count) > 1:
            return message % {"count": _("%s instances") % count,
                              "name": name}
        else:
            return message % {"count": _("instance"), "name": name}

    def handle(self, request, context):
        dev_mapping = None
        netids = context.get('network_id', None)
        if netids:
            nics = [{"net-id": netid, "v4-fixed-ip": ""}
                    for netid in netids]
        else:
            nics = None
        

        template = 'custom/hadoop/cloud-config/hadoop.template'
        hadoop_group_id = genId()

        data = {}
        data.update(getEc2Keys(request))
        data["tenant_id"]=request.user.tenant_id
        data["user_id"]=request.user.id
        data["container_name"]=getCustomContainerName(request)
        data["hadoop_group_id"]=hadoop_group_id
        data["hadoop_master_id"]=""
        data.update(context)
        custom_script = render_to_string(template,data)
        try:
            master = api.nova.server_create(request,
                                   context['name']+'-master',
                                   context['source_id'],
                                   context['master_flavor'],
                                   context['keypair_id'],
                                   normalize_newlines(custom_script),
                                   context['security_group_ids'],
                                   dev_mapping,
                                   nics=nics,
                                   instance_count = 1,
                                   admin_pass=context['admin_pass'])
            master = api.nova.server_get(request, master.id)
            data["hadoop_master_id"] = master.id
            data["hadoop_master_name"] = master.internal_name
            custom_script = render_to_string(template,data)

            del  data["confirm_admin_pass"]
            del  data["admin_pass"]
            setHadoopGroup(request,data)

            api.nova.server_create(request,
                                   context['name']+'-slave',
                                   context['source_id'],
                                   context['flavor'],
                                   context['keypair_id'],
                                   normalize_newlines(custom_script),
                                   context['security_group_ids'],
                                   dev_mapping,
                                   nics=nics,
                                   instance_count = int(context['count']),
                                   admin_pass=context['admin_pass'])

        except:
            exceptions.handle(request)
            return False

        return True



class CreateView(workflows.WorkflowView):
    workflow_class = CreateHadoopGroup

    def get_initial(self):
        pass




class JobAction(workflows.Action):
    group_id = forms.Field(label=("group_id"),
                     required=True,
                     help_text="group id", 
                     widget=forms.TextInput(
                         attrs={'readonly': 'readonly'}),
    )
 
    name = forms.Field(label=("job_name"),
                     required=False,
                     help_text="Hadoop job name.", )
    jar_location = forms.Field(label=("jar location"),
                     required=False,
                     help_text="jar location", )

    jar_args = forms.Field(label=("jar arguments"),
                     required=False,
                     help_text="jar arguments", )


    def handle(self, request, data):
        return True

    class Meta:
        name = "Job"

class JobStep(workflows.Step):
    action_class = JobAction
    depends_on = ("group_id",)
    contributes = ("name","jar_location" ,"jar_args")

class CreateHadoopJob(workflows.Workflow):
    slug = "workflow"
    name = "HadoopJobWorkFlow"
    finalize_button_name = "Save"
    success_message = 'success_message "{name}".'
    failure_message = 'failure_message "%s".'
    default_steps = (JobStep ,)

    def get_link_url(self, datum=None):
        return reverse(self.url, args=(self.table.kwargs["group_id"] ,))

    def format_status_message(self, message):
        name = self.context.get('job_name') or self.context.get('job_id')
        return message % name

    def get_success_url(self):
        return reverse("horizon:custom:hadoop:group",
                       args=(self.context.get("group_id"),))

    def get_failure_url(self):
        return reverse("horizon:custom:hadoop:group",
                       args=(self.context.get("group_id"),))

    def format_status_message(self, message):
        return message.format(**self.context)

    def handle(self, request, context):
        import uuid
        new_id = str(uuid.uuid1())
        data = context
        data.update({"id":new_id})
        setHadoopJob(request,data)
        return True


class CreateJobView(workflows.WorkflowView):
    workflow_class = CreateHadoopJob

    def get_initial(self):
        return {"group_id": self.kwargs['group_id'],
                }



class LaunchGroupLink(tables.LinkAction):
    name = "create"
    verbose_name = _("Create Hadoop Group ")
    url = "horizon:custom:hadoop:create"
    classes = ("ajax-modal", "btn-create")
    

def getHadoopGroup(request,group_id):
    key=CUSTOM_HADOOP_PREFIX+"."+group_id
    container_name=getCustomContainerName(request)
    return getObj(request,container_name,key)


def getHadoopInstance(request,group_id,instance_id):
    key=CUSTOM_HADOOP_PREFIX+group_id+"/hosts/"+instance_id
    container_name=getCustomContainerName(request)
    return getObj(request,container_name,key)


class HadoopIndexUpdateRow(tables.Row):
    ajax = True
    def get_data(self, request,id):
        datum = json.loads(getHadoopGroup(request,id))
        return datum


class HadoopIndexTable(tables.DataTable):
    class Meta:
        name = "Hadoop Group"
        status_columns = ["ajax_state" , ]
        table_actions = (LaunchGroupLink , )
        row_class = HadoopIndexUpdateRow


    hadoop_group_id = tables.Column( 
                        "hadoop_group_id" , 
                        verbose_name=_("id"),
                        link=("horizon:custom:hadoop:group"),
                        )

    AJAX_STATE = (
        ("ok", True),
    )
    def getState(datum):
        if datum.has_key("hadoop_master_id"):
            return "ok"
        return None
    ajax_state = tables.Column(getState ,
                        verbose_name="State",
                         status=True,
                         status_choices=AJAX_STATE,
                        hidden=True,
                )

    def getName(datum):
        if datum.has_key("hadoop_master_id"):
            return datum.get("name",None)
        return None
    name = tables.Column(getName,
                         verbose_name=_("Group Name"))


    def getCount(datum):
        return datum.get("count",None)

    count = tables.Column(getCount,
                         verbose_name=_("Slave Count"))




    def get_object_id(self, datum):
        return datum["hadoop_group_id"]
    def get_object_display(self, datum):
        return datum["hadoop_group_id"]

from django.utils.http import urlencode
from openstack_dashboard.dashboards.project.instances.tables import  POWER_STATES
class  HadoopGroupUpdateRow(tables.Row):
    ajax = True
    def get_data(self, request,instance_id):
        func, args, kwargs = resolve(request.META["PATH_INFO"])
        datum = json.loads( getHadoopInstance(request,kwargs['group_id'],instance_id))
        try:
            instance = api.nova.server_get(request, instance_id)
            datum['state'] = POWER_STATES.get(getattr(instance, "OS-EXT-STS:power_state", 0), '')
        except:
            pass
        return datum

    def get_ajax_update_url(self):
        table_url = self.table.get_absolute_url()
        
        params = urlencode({"table": self.table.name,
                            "action": self.ajax_action_name,
                            "obj_id": self.table.get_object_id(self.datum),
                                })
        return "%s?%s" % (table_url, params)

class BtnCreateJob(tables.LinkAction):
    name = "create_job"
    verbose_name = "Create Job"
    classes = ("ajax-modal", "btn-create")
    url = "horizon:custom:hadoop:create_job"

    def get_link_url(self, datum=None):
        return reverse(self.url, args=(self.table.kwargs["group_id"] ,))

class HadoopGroupTable(tables.DataTable):
    class Meta:
        name = "Hadoop Group"
        table_actions = ()
        status_columns = ["ajax_state" , ]
        table_actions = ( BtnCreateJob , )
        row_class = HadoopGroupUpdateRow


    uuid = tables.Column("uuid",
                         verbose_name=_("uuid"))

    AJAX_STATE = (
        ("ok", True),
    )
    def getState(datum):
        if datum.has_key("id"):
            return "ok"
        return None
    ajax_state = tables.Column(getState ,
                        verbose_name="State",
                        status=True,
                        status_choices=AJAX_STATE,
                        hidden=True,
                )


    def getName(datum):
        return datum.get("name",None)
    
    name = tables.Column(getName,
                         link=("horizon:custom:hadoop:detail"),
                         verbose_name=_("Name"))
    def getState(datum):
        return datum.get("state",None)
    
    def get_update_time(datum):
        return datum.get("update_time",None)
    
    def getEc2_id(datum):
        return datum.get("id","unknow")
    
    def getAddress(datum):
        ip = datum.get("ip_address","unknow")
        _ip = datum.get("private_ip_address","unknow")
        if ip ==_ip:
            return ip
        else:
            return "%s , %s"%(_ip,ip)


    ec2_id = tables.Column(getEc2_id,
                         link=("horizon:custom:hadoop:detail"),
                         verbose_name=_("EC2"))

    state = tables.Column(getState ,
                        verbose_name="State",
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
        return datum["uuid"]

class IndexView(tables.DataTableView):
    table_class = HadoopIndexTable
    template_name = 'custom/hadoop/tables.html'
    def get_data(self):
        request = self.request
        container_name = getCustomContainerName(request)
        return getHadoopGroupList(request)

class GroupView(tables.DataTableView):
    table_class = HadoopGroupTable
    template_name = 'custom/hadoop/tables.html'
    def get_data(self):
        request = self.request
        data = getHadoopInstancesList(request,self.kwargs['group_id'])
        return data


