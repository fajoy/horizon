from django.utils.translation import ugettext_lazy as _
from horizon import exceptions
from openstack_dashboard import api


from django.http import HttpResponse

from openstack_dashboard import api
from horizon import tables

from horizon import messages
from horizon import exceptions


from horizon import workflows
from horizon import forms
from django.core.urlresolvers import reverse

import uuid
import json

import logging
log = logging.getLogger("Custom")


CUSTOM_JOB_PREFIX= ".job/" 

def getCustomJobFlowKey(datum):
    return "%s%s"%(CUSTOM_JOB_PREFIX,datum["id"])

def getObj(request,container_name,key):
    obj = api.swift.swift_get_object(
        request,
        container_name,
        key
        )
    return json.loads(obj.data)

def setObj(request,container_name,key,obj):
        object_name = key
        object_file = json.dumps(obj)
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

def saveCustomJobFlow(request,datum):
    container_name=getCustomContainerName(request)
    createContainer(request,container_name)
    key=getCustomJobFlowKey(datum)
    setObj(request,container_name,key,datum)

def getCustomStatusData(request,table):
    data=[]
    container_name=getCustomContainerName(request)
    items = getObjList(request,container_name,prefix=CUSTOM_JOB_PREFIX)
    for item in items:
        key=item["name"]
        datum=getObj(request,container_name,key)
        data.append(datum)
    return data

class DeleteJobFlow(tables.BatchAction):
    name = "delete"
    action_present = _("Delete")
    action_past = _("Delete of")
    data_type_singular = _("JobFlow")
    data_type_plural = _("JobFlows")
    classes = ('btn-danger', 'btn-terminate')
    def __init__(self):
        super(DeleteJobFlow, self).__init__()

    def allowed(self, request, datum=None):
        return True

    def action(self, request, obj_id):
        datum=self.table.get_object_by_id(obj_id)
        datum["state"] = "deleted"
        saveCustomJobFlow(request,datum)
        

class CreateCustomJobFlowInfoAction(workflows.Action):
    job_name = forms.CharField(max_length=255,
                               label=_("Job Flow Name"),
                               initial="My Job Flow",
                               help_text=_("Input disaplay Job Flow name."
                                           ""),
                               required=True
                                )

    def __init__(self, request, *args, **kwargs):
        super(CreateCustomJobFlowInfoAction, self).__init__(request, *args, **kwargs)

    """
    flavor = forms.ChoiceField(label=_("Flavor"),
                               help_text=_("Size of image to launch."))
    count = forms.IntegerField(label=_("Instance Count"),
                               min_value=1,
                               initial=1,
                               help_text=_("Number of instances to launch."))


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
"""

    class Meta:
        name = _("Custom Job")
        help_text = _("Create Custom Flow.\n"
                      "")

class CreateCustomJobFlowJarAction(workflows.Action):
    jar_container = forms.CharField(max_length=255,
                               label=_("Container"),
                               initial="jar Container",
                               help_text=_("Jar Container"
                                           ""),
                               required=True
                                )
    jar_location = forms.CharField(max_length=255,
                               label=_("Job Location"),
                               help_text=_("jar path "
                                           ""),
                               required=True
                                )
    jar_argument = forms.CharField(widget=forms.Textarea,
                               label=_("Jar argument"),
                               help_text=_("custom jar argument"
                                           ""),
                               required=False
                                )



    def __init__(self, request, *args, **kwargs):
        super(CreateCustomJobFlowJarAction, self).__init__(request, *args, **kwargs)

    class Meta:
        name = _("Custom Set Jar")
        help_text = _("Create Custom Flow.\n"
                      "")



class CreateJobFlowDetail(workflows.Step):
    action_class = CreateCustomJobFlowInfoAction
    contributes = ( 'job_name',
                    )

    
class CreateJobFlowJar(workflows.Step):
    action_class = CreateCustomJobFlowJarAction
    contributes = (
                     'jar_container',
                     'jar_location',
                     'jar_argument',
                    )


class CreateJobFlow(workflows.Workflow):
    slug = "create"
    name = _("Create JobFlow")
    finalize_button_name = _("Create")
    success_message = _('Created JobFlow "%s".')
    failure_message = _('Unable to create JobFlow "%s".')
    default_steps = (
                        CreateJobFlowDetail ,
                        CreateJobFlowJar
                     )

    def get_success_url(self):
        return reverse("horizon:custom:status:index")


    def get_failure_url(self):
        return reverse("horizon:custom:status:index")

    def format_status_message(self, message):
        name = self.context["job_name"]
        return message % name
    def handle(self, request, context):
        context["id"]=genId()
        context["create_time"] = now()
        context["state"] = "init"
        saveCustomJobFlow(request,context)
        """
        import json
        print json.dumps(context)
        custom_meta_name="custom_meta_"+request.user.tenant_id
        container_name=custom_meta_name
        object_name="test.json"
        object_file=json.dumps(context)
        size=len(object_file)
        headers = {}
        headers['X-Object-Meta-Orig-Filename'] = "custom"
        etag = api.swift.swift_api(request).put_object(container_name,
                                         object_name,
                                         object_file,
                                         content_length=size,
                                         headers=headers)
        obj_info = {'name': object_name, 'bytes': object_file.size, 'etag': etag}
"""
        return True



class CreateView(workflows.WorkflowView):
    workflow_class = CreateJobFlow

    def get_initial(self):
        pass



class LaunchCustomLink(tables.LinkAction):
    name = "create"
    verbose_name = _("Create Custom Job")
    url = "horizon:custom:status:create"
    classes = ("ajax-modal", "btn-create")
    



class StatusTable(tables.DataTable):
    name = tables.Column('job_name')
    state = tables.Column('state')
    create_time = tables.Column('create_time')
        
    def get_object_id(self, datum):
        return datum["id"]
    def get_object_display(self, datum):
        return datum["job_name"]
    class Meta:
        name = "Custom Status"
        table_actions = (LaunchCustomLink , DeleteJobFlow )
        row_actions = (DeleteJobFlow,)




class IndexView(tables.DataTableView):
    table_class = StatusTable
    template_name = 'custom/hadoop/tables.html'
    def get_data(self):
        request = self.request
        data = getCustomStatusData(request,self.table)
        return data
#        return [{'state': 'init', 'create_time': '2013-06-09 15:11:04', 'job_name': u'My Job Flow', 'id': 'cdef8f6e-d116-11e2-b754-001fc6f16ad5'} ,] 


