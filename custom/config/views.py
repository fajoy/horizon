from django.utils.translation import ugettext_lazy as _
from django.http import HttpResponse
from django.core.urlresolvers import reverse

from horizon import exceptions
from horizon import messages
from horizon import exceptions
from horizon import forms

from openstack_dashboard import api
import uuid
import json

import logging
log = logging.getLogger("Custom")

CUSTOM_CONF_PREFIX= ".config/" 
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

def setObj(request,container_name,key,obj):
        object_name = key
        object_file=obj
        size = len(object_file)
        headers = {}
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


def getKeys(request):
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



def saveScript(request,name,text):
    key=CUSTOM_CONF_PREFIX+name
    value=text
    container_name=getCustomContainerName(request)
    createContainer(request,container_name)
    setObj(request,container_name,key,value)


def getScript(request,name):
    key=CUSTOM_CONF_PREFIX+name
    container_name=getCustomContainerName(request)
    return getObj(request,container_name,key)


from django.template import Context, Template
def getTemplateScriptRender(request,name):
    script = getScript(request,name)
    c = getKeys(request)
    c["tenant_id"]=request.user.tenant_id
    c["user_id"]=request.user.id
    c["container_name"]=getCustomContainerName(request)
    """
    {{ ec2_access_key }}
    {{ ec2_secret_key }}
    {{ tenant_id }}
    {{ user_id }}
    {{ container_name }}
    """
    print c
    t = Template(script)

    return  t.render(Context(c))

def delScript(request,name):
    key=CUSTOM_CONF_PREFIX+name
    container_name=getCustomContainerName(request)
    delObj(request,container_name,key)

def getScriptList(request):
    data=[]
    container_name=getCustomContainerName(request)
    items = getObjList(request,container_name,prefix=CUSTOM_CONF_PREFIX)
    for item in items:
        item["name"]=item["name"][len(CUSTOM_CONF_PREFIX):]
    return items



from horizon import forms

class ScriptForm(forms.SelfHandlingForm):
    name = forms.RegexField(
                            max_length="255", 
                            label=_("Name"), 
                            required=True,
                            help_text=_("script name,only use [a-zA-Z0-9_] word."),
                            regex=r"^\w+$"
                            )
    customization_script = forms.CharField(
                                           widget=forms.Textarea({'style':'margin: 0px 0px 0px; height: 300px; width: 600px;' }),
                                           label=_("Customization Script"),
                                           required=False,
                                           help_text=_("A script or set of "
                                                       "commands to be "
                                                       "executed after the "
                                                       "instance has been "
                                                       "built (max 16kb)."))
    class Meta:
        pass
    def __init__(self, *args, **kwargs):
        super(ScriptForm, self).__init__(*args, **kwargs)

    def clean(self):
        data = super(ScriptForm, self).clean()
        return data

    def handle(self, request, data):
        key=data["name"]
        value=data["customization_script"]
        saveScript(request,key,value)
        return data

from horizon import views
class EditScriptView(forms.ModalFormView):
    form_class = ScriptForm
    template_name = 'custom/config/script_form.html'
    success_url = "horizon:custom:config:index"

    def get_success_url(self):
        return reverse(self.success_url)

    def get_initial(self):
        initial = super(EditScriptView, self).get_initial()
        initial["name"] = self.kwargs["name"]
        initial["customization_script"] = getScript(self.request,self.kwargs["name"])
        return initial

    def get_context_data(self, **kwargs):
        context = super(EditScriptView, self).get_context_data(**kwargs)
        context['name'] = self.kwargs["name"]
        context["var_args"]="""
    {{ ec2_access_key }}
    {{ ec2_secret_key }}
    {{ tenant_id }}
    {{ user_id }}
    {{ container_name }}
"""
        return context

class CreateScriptView(forms.ModalFormView):
    form_class = ScriptForm
    template_name = 'custom/config/script_form.html'
    success_url = "horizon:custom:config:index"

    def get_success_url(self):
        return reverse(self.success_url)

    def get_initial(self):
        initial = super(CreateScriptView, self).get_initial()
        initial["name"] = ""
        return initial

    def get_context_data(self, **kwargs):
        context = super(CreateScriptView, self).get_context_data(**kwargs)
        context["var_args"]="""
    {{ ec2_access_key }}
    {{ ec2_secret_key }}
    {{ tenant_id }}
    {{ user_id }}
    {{ container_name }}
"""
        return context

class ShowScriptView(forms.ModalFormView):
    form_class = ScriptForm
    template_name = 'custom/config/show_script.html'
    success_url = "horizon:custom:config:index"

    def get_success_url(self):
        return reverse(self.success_url)

    def get_initial(self):
        initial = super(ShowScriptView, self).get_initial()
        initial["name"] = ""
        initial["customization_script"] = ""
        return initial

    def get_context_data(self, **kwargs):
        context = super(ShowScriptView, self).get_context_data(**kwargs)
        context["name"]=self.kwargs["name"]
        context["script"]= getTemplateScriptRender(self.request,self.kwargs["name"])
        return context

from horizon import tables
from django import shortcuts
class DeleteScript(tables.BatchAction):
    name = "delete"
    action_present = _("Delete")
    action_past = _("Delete of")
    data_type_singular = _("Script")
    data_type_plural = _("Script")
    classes = ('btn-danger', 'btn-terminate')
    def __init__(self):
        super(DeleteScript, self).__init__()

    def allowed(self, request, datum=None):
        return True

    def action(self, request, obj_id):
        name=obj_id
        delScript(request,name)
        return shortcuts.redirect(self.get_success_url(request))

    def get_success_url(self, request=None):
        return request.get_full_path()

 
class CreateScript(tables.LinkAction):
    name = "create"
    verbose_name = _("Create Script")
    url = "horizon:custom:config:create_script"
    classes = ("ajax-modal", "btn-create")


class EditScript(tables.LinkAction):
    name = "edit"
    verbose_name = _("Edit")
    url = "horizon:custom:config:edit_script"
    classes = ("ajax-modal", "btn-create")
    def get_link_url(self, datum):
        return reverse(self.url, args=(datum["name"] ,))

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
    success_url = "horizon:project:instances:index"
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


class LaunchLink(tables.LinkAction):
    name = "launch"
    verbose_name = _("Launch Instance")
    url = "horizon:custom:config:launch"
    classes = ("ajax-modal", "btn-create")
 


class ScriptTable(tables.DataTable):
    name = tables.Column('name',
            link=("horizon:custom:config:show_script"),
            link_classes= ( "ajax-modal",)
            )
    size = tables.Column('bytes',
            verbose_name="Size")
    update = tables.Column('last_modified' ,
            verbose_name="Update")
    def get_object_id(self, datum):
        return datum["name"]
    def get_object_display(self, datum):
        return datum["name"]
    class Meta:
        name = "script_list"
        verbose_name = "Customization Script"
        table_actions = (CreateScript , DeleteScript , LaunchLink )
        row_actions = ( EditScript ,DeleteScript  ,)



from horizon import tabs
class ScriptTab(tabs.TableTab):
    table_classes = ( ScriptTable , )
    template_name = "horizon/common/_detail_table.html"
    name = _("Script")
    verbose_name = "Script"
    slug = "script_tab"
    def get_script_list_data(self):
        return getScriptList(self.request)
#        return [{'bytes': 5, 'last_modified': '2013-06-14T15:26:54.000Z', 'hash': '86afd3f08f26ec09d6afdede939ae84a', 'name': '.config/hello'},]
    
class GroupTabs(tabs.TabGroup):
    slug = "tabs"
    tabs = (ScriptTab , )
    sticky = True

class IndexView(tabs.TabbedTableView):
    tab_group_class = GroupTabs
    template_name = 'custom/config/tabs.html'

