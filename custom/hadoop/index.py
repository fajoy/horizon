from django.utils.translation import ugettext_lazy as _
from django.core.urlresolvers import reverse
from django import shortcuts
from django.utils.text import normalize_newlines
from horizon import tables
from horizon import messages
from horizon import exceptions
from horizon import exceptions
from horizon import workflows
from horizon import forms
from horizon.utils import validators
from openstack_dashboard import api
from openstack_dashboard.dashboards.project.instances.workflows import KEYPAIR_IMPORT_URL
from openstack_dashboard.dashboards.project.instances.workflows import SetNetwork 
from openstack_dashboard.dashboards.project.images_and_snapshots.utils import get_available_images
from openstack_dashboard.usage import quotas
import json
from ..hadoop.api import hadoop

def get_ec2_keys(request):
    tenant_id = request.user.tenant_id
    try:
        all_keys = api.keystone.list_ec2_credentials(request,
                                                     request.user.id)
        keys = None
        return dict((key.access ,key.secret) for key in all_keys if key.tenant_id == tenant_id)
    except:
        exceptions.handle(request,
                          _('Unable to fetch EC2 credentials.'),
                          redirect=request.build_absolute_uri())
    return []

class CreateaEc2KeyForm(forms.SelfHandlingForm):
    def handle(self, request, data):
        tenant_id = request.user.tenant_id
        try:
            keys = api.keystone.create_ec2_credentials(request,
                                                       request.user.id,
                                                       tenant_id)

            messages.success(request, _('Successfully Create EC2 key: %s')
                                       % keys.access)
            return keys
        except:
            exceptions.handle(request, ignore=True)
            self.api_error(_('Unable to Create Key.'))
            return False

class CreateEc2KeyView(forms.ModalFormView):
    form_class = CreateaEc2KeyForm
    template_name = 'custom/hadoop/create_ec2_key_form.html'
    success_url = 'horizon:custom:hadoop:index'
    def get_success_url(self):
        return reverse(self.success_url)
    def get_object_display(self, datum):
        return datum.access 
    def get_object_id(self, datum):
        return datum.access 

class SetHadoopMasterDetailsAction(workflows.Action):
    source_id = forms.ChoiceField(label=_("Image"), required=True)
    name = forms.CharField(max_length=80, label=_("Group Name"))
    flavor = forms.ChoiceField(label=_("Master Instance Flavor"),
                               help_text=_("Size of image to launch."))

    class Meta:
        name = _("Details")
        help_text_template = ("project/instances/"
                              "_launch_details_help.html")

    def clean(self):
        cleaned_data = super(SetHadoopMasterDetailsAction, self).clean()
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
                      and str(image.id) in hadoop.get_image_list(request)]  
        if choices:
            if len(choices) == 1:
                self.fields['source_id'].initial = choices[0][0]
            choices.insert(0, ("", _("Select a Image")))
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
        return super(SetHadoopMasterDetailsAction, self).get_help_text(extra)

class SetAccessControlsAction(workflows.Action):
    ec2_key = forms.DynamicChoiceField(label=_("EC2 Key"),
                                       required=True,
                                       help_text=_("Which keypair to use for "
                                                   "authentication."),
                                       add_item_link="horizon:custom:hadoop:create_ec2"
                                       )
 
    keypair = forms.DynamicChoiceField(label=_("Keypair"),
                                       required=False,
                                       help_text=_("Which keypair to use for "
                                                   "authentication."),
                                       add_item_link=KEYPAIR_IMPORT_URL)
    admin_pass = forms.RegexField(
            label=_("Root Password"),
            required=False,
            widget=forms.PasswordInput(render_value=False),
            regex=validators.password_validator(),
            error_messages={'invalid': validators.password_validator_msg()})
    confirm_admin_pass = forms.CharField(
            label=_("Confirm Root Password"),
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

    def populate_ec2_key_choices(self, request, context):
        keys = get_ec2_keys(request)
        _list = [(access ,access)for access in keys]
        if _list:
            if len(_list) == 1:
                self.fields['ec2_key'].initial = _list[0][0]
            _list.insert(0, ("", _("Select a keypair")))
        else:
            _list = (("", _("No EC2 key available.")),)
        return _list

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

class SetHadoopMasterDetails(workflows.Step):
    action_class = SetHadoopMasterDetailsAction
    contributes = ( "source_id", "name", "flavor")
    class Meta:
        name = _("Details")
        help_text_template = ("project/instances/"
                              "_launch_details_help.html")

class SetAccessControls(workflows.Step):
    action_class = SetAccessControlsAction
    contributes = ("ec2_access_key","ec2_secret_key","keypair_id", "security_group_ids",
            "admin_pass", "confirm_admin_pass")

    def contribute(self, data, context):
        if data:
            keys = get_ec2_keys(self.workflow.request)
            access = data.get("ec2_key","")
            context['ec2_access_key']= access
            context['ec2_secret_key']= keys.get(access,"")
            post = self.workflow.request.POST
            context['security_group_ids'] = post.getlist("groups")
            context['keypair_id'] = data.get("keypair", "")
            context['admin_pass'] = data.get("admin_pass", "")
            context['confirm_admin_pass'] = data.get("confirm_admin_pass", "")
        return context


class UserScriptAction(workflows.Action):
    class Meta:
        name = "User-Data Scripts"

    user_script =  forms.Field( widget=forms.Textarea({'style':'margin: 0px 0px 0px; height: 200px;' }),
                            label=_("User-Data Scripts"),
                            required=False,
                            )
    def __init__(self, request, context, *args, **kwargs):
        super(UserScriptAction, self).__init__(request, context, *args, **kwargs)
        self.fields["user_script"].initial=context.get("user_script","""""")

    def handle(self, request, data):
        return True

    def get_help_text(self, extra_context=None):
        return """
As popularized by alestic.com, user-data scripts are a convenient way to do something on first boot of a launched instance. This input format is accepted to cloud-init and handled as you would expect. The script will be invoked at an "rc.local" like point in the boot sequence.
<br />
<br />
Example:
<pre>
#!/bin/bash
apt-get install -y git
</pre>

You want more custom modify, you can download Horizon <a href="https://github.com/fajoy/horizon">Source Code</a>,
instal and modify init_script.
"""

class UserScriptStep(workflows.Step):
    slug="user_script"
    action_class = UserScriptAction
    contributes = ("user_script" , )
    def contribute(self, data, context):
        context.update(data)
        return context



class CreateMasterWorkflow(workflows.Workflow):
    slug = "create_group"
    name = _("Create Hadoop Master")
    finalize_button_name = _("Create")
    success_message = _('Created Hadoop Master "%s".')
    failure_message = _('Unable to create Hadoop Master "%s".')
    success_url = "horizon:custom:hadoop:group:index"
    default_steps = (
                     SetHadoopMasterDetails,
                     SetAccessControls,
                     SetNetwork,
                     UserScriptStep,
                    )

    def format_status_message(self, message):
        name = self.context.get('name', 'unknown instance')
        return message % ( name ,)

    def handle(self, request, context):
        context["user_script"]=normalize_newlines(context["user_script"])
        meta=hadoop.create_master(request,context)
        if meta==None:
            return False
        self.context.update(meta)
        return True
    def get_success_url(self):
        return reverse("horizon:custom:hadoop:group:index",
                       args=(self.context.get("hadoop_group_id"),))

    def get_failure_url(self):
        return reverse("horizon:custom:hadoop:index",)


class CreateMasterView(workflows.WorkflowView):
    workflow_class = CreateMasterWorkflow

    def get_initial(self):
        pass

class CreateMasterAction(tables.LinkAction):
    name = "create"
    verbose_name = _("Create Hadoop Master ")
    url = "horizon:custom:hadoop:create"
    classes = ("ajax-modal", "btn-create")

class DeleteGroupAction(tables.BatchAction):
    name = "Delete Group"
    action_present = _("Delete")
    action_past = _("Delete of")
    data_type_singular = _("Group")
    data_type_plural = _("Group")
    classes = ('btn-danger', 'btn-terminate')
    def __init__(self):
        super(DeleteGroupAction, self).__init__()

    def allowed(self, request, datum=None):
        return True

    def action(self, request, group_id):
        hadoop.delete_group(request,group_id)
        return shortcuts.redirect(self.get_success_url(request))

    def get_success_url(self, request=None):
        return request.get_full_path()

class UpdateRow(tables.Row):
    ajax = True
    def get_data(self, request,id):
        datum = hadoop.get_group_meta(request,id)
        datum["request"]= request
        if not request.session.has_key("instances_cache"):
            instances,_more = api.nova.server_list(request)
            request.session["instances_cache"]= dict((instance.id,dict(((attr,getattr(instance,attr,None)))  for attr in instance._attrs))for instance in instances)
        
        instances_cache = request.session["instances_cache"]
        meta_list= hadoop.get_instance_list(request,datum["hadoop_group_id"])
        datum["instance_count"]= str(len(meta_list))
        meta_ids= set( meta["uuid"] for meta in meta_list)
        nova_ids= set(instances_cache.keys())
        datum["live_count"]=len( meta_ids & nova_ids)
        return datum

class Table(tables.DataTable):
    class Meta:
        name = "Hadoop Group"
        status_columns = ["ajax_state" , ]
        table_actions = (CreateMasterAction ,  DeleteGroupAction)
        row_actions = (DeleteGroupAction ,)
        row_class = UpdateRow

    def get_name(datum):
        return datum.get("name",None)
    name = tables.Column(get_name,
                        verbose_name=_("Group Name"),
                        link=("horizon:custom:hadoop:group:index"),
                        )


    def get_instance_count(datum):
        if not datum.has_key("instance_count"):
            return "-"
        return datum["instance_count"]

    def get_live_count(datum):
        if not datum.has_key("live_count"):
            return "-"
        return datum["live_count"]

    def get_job_count(datum):
        if not datum.has_key("request"):
            return "-"
        job_list = hadoop.get_job_list(datum["request"],datum["hadoop_group_id"])
        return "Job Count(%d)"%len(job_list)

    job_count = tables.Column(get_job_count,
                        verbose_name=_("Create Job Count"),
                        link=("horizon:custom:hadoop:job:index"),
                        )


    instace_count = tables.Column(get_instance_count,
                        verbose_name=_("Create Instance Count"),
                        )

    live_count = tables.Column(get_live_count,
                         verbose_name=_("Live Instance Count"))

    def get_ajax_state(datum):
        if datum.has_key("request"):
            return "true"
        return None
    ajax_state = tables.Column(get_ajax_state ,
                        verbose_name="state",
                        status=True,
                        hidden=True,
                )

    def get_object_id(self, datum):
        return datum["hadoop_group_id"]
    def get_object_display(self, datum):
        return datum["name"]

class IndexView(tables.DataTableView):
    table_class = Table
    template_name = 'custom/hadoop/tables.html'
    def get_data(self):
        return hadoop.get_group_list(self.request)
