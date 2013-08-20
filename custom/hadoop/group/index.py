from django.utils.translation import ugettext_lazy as _
from django.core.urlresolvers import reverse
from django.core.urlresolvers import resolve

from horizon.utils import validators
from horizon import tables
from horizon import messages
from horizon import exceptions
from horizon import workflows
from horizon import forms

from openstack_dashboard.dashboards.project.instances.workflows import SetInstanceDetailsAction,KEYPAIR_IMPORT_URL
from openstack_dashboard.dashboards.project.instances.workflows import LaunchInstance, SetInstanceDetails, SetNetwork
from openstack_dashboard.dashboards.project.images_and_snapshots.utils import get_available_images
from openstack_dashboard.dashboards.project.instances.tables import  POWER_STATES
from openstack_dashboard.usage import quotas
from openstack_dashboard import api

from contextlib import closing
import tempfile
import zipfile
import uuid
import json
from ...hadoop.api import hadoop

class SetSlaveDetailsAction(workflows.Action):
    source_id = forms.ChoiceField(label=_("Image"), required=True)
    flavor = forms.ChoiceField(label=_("Slave Instance Flavor"),
                               help_text=_("Size of image to launch."))

    count = forms.IntegerField(label=_("Slave Instance Count"),
                               min_value=1,
                               initial=1,
                               help_text=_("Number of instances to launch."))

    class Meta:
        name = _("Details")
        help_text_template = ("project/instances/"
                              "_launch_details_help.html")

    def clean(self):
        cleaned_data = super(SetSlaveDetailsAction, self).clean()
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
        return super(SetSlaveDetailsAction, self).get_help_text(extra)


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

class SetSlaveDetails(workflows.Step):
    action_class = SetSlaveDetailsAction
    contributes = ( "source_id",  "flavor", "count",)
    class Meta:
        name = _("Details")
        help_text_template = ("project/instances/"
                              "_launch_details_help.html")

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

class CreateSlaveWorkflow(workflows.Workflow):
    slug = "create_group"
    name = _("Create Hadoop Slave")
    finalize_button_name = _("Create")
    success_message = _('Created Hadoop Slave Success.')
    failure_message = _('Unable to Create Hadoop Slave "%s".')
    success_url = "horizon:custom:hadoop:index"
    default_steps = (
                     SetSlaveDetails,
                     SetAccessControls,
                    )

    def format_status_message(self, message):
        return message 

    def handle(self, request, context):
        func, args, kwargs = resolve(request.META["PATH_INFO"])
        meta = hadoop.create_slave(request,kwargs["group_id"],context)
        if meta== None:
            return False
        return True

    def get_success_url(self):
        func, args, kwargs = resolve(self.request.META["PATH_INFO"])
        return reverse("horizon:custom:hadoop:group:index",
                       args=(kwargs["group_id"],))

    def get_failure_url(self):
        return reverse("horizon:custom:hadoop:index",)


class CreateSlaveView(workflows.WorkflowView):
    workflow_class = CreateSlaveWorkflow

    def get_initial(self):
        pass

class CreateSlaveAction(tables.LinkAction):
    name = "create_slave"
    verbose_name = "Create Slave"
    classes = ("ajax-modal", "btn-create")
    url = "horizon:custom:hadoop:group:create_slave"

    def get_link_url(self, datum=None):
        return reverse(self.url, args=(self.table.kwargs["group_id"] ,))


class CreateJarAction(tables.LinkAction):
    name = "create_jarjob"
    verbose_name = "Create Job"
    classes = ("ajax-modal", "btn-create")

    def get_link_url(self, datum=None):
        url = "horizon:custom:hadoop:job:create_jar"
        return reverse(url, args=(self.table.kwargs["group_id"] ,))

class JobListAction(tables.LinkAction):
    name = "job_list"
    verbose_name = "Job List"
    classes = ( "btn" , )

    def get_link_url(self, datum=None):
        return reverse('horizon:custom:hadoop:job:index', args=(self.table.kwargs["group_id"] , ))


class UpdateRow(tables.Row):
    ajax = True
    def get_data(self, request,instance_id):
        func, args, kwargs = resolve(request.META["PATH_INFO"])
        datum = hadoop.get_instance_meta(request,kwargs['group_id'],instance_id)
        try:
            instance = api.nova.server_get(request, instance_id)
            datum['state'] = POWER_STATES.get(getattr(instance, "OS-EXT-STS:power_state", 0), '')
        except:
            self.table.disable_column_link()
            datum['state'] = "Terminated"
            pass
        datum["request"]=request
        return datum



class Table(tables.DataTable):
    class Meta:
        name = "Hadoop Group"
        table_actions = ()
        status_columns = ["ajax_state" , ]
        table_actions = ( JobListAction, CreateSlaveAction, )
        row_class = UpdateRow


    uuid = tables.Column("uuid",
                         verbose_name=_("uuid"))

    def getState(datum):
        if datum.has_key("request"):
            return "true"
        return None
    ajax_state = tables.Column(getState ,
                        verbose_name="State",
                        status=True,
                        hidden=True,
                )


    def get_name(datum):
        if datum.get("state",None):
            return datum.get("name",None)
        return None
    
    name = tables.Column(get_name,
                         link=("horizon:custom:hadoop:detail"),
                         verbose_name=_("Name"))
    def getState(datum):
        return datum.get("state",None)
    
    def getAddress(datum):
        ip = datum.get("ip_address","unknow")
        _ip = datum.get("private_ip_address","unknow")
        if ip ==_ip:
            return ip
        else:
            return "%s , %s"%(_ip,ip)

    def disable_column_link(self):
        cols = self.columns
        for key in cols:
            cols[key].link=None

    state = tables.Column(getState ,
                        verbose_name="State",
                )
    address = tables.Column(getAddress ,
                        verbose_name="Address",
                )

    def get_object_id(self, datum):
        return datum["uuid"]
    def get_object_display(self, datum):
        return datum["uuid"]


class GroupView(tables.DataTableView):
    table_class = Table
    template_name = 'custom/hadoop/tables.html'
    def get_data(self):
        request = self.request
        return hadoop.get_instance_list(request,self.kwargs['group_id'])

