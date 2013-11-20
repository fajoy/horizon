from django.utils.translation import ugettext_lazy as _
from django.core.urlresolvers import reverse
from django.core.urlresolvers import resolve
from django.core import urlresolvers
from django import template
from django.template import Context, Template
from django.utils.http import urlencode
from django import shortcuts
from django.core.cache import cache

from horizon.templatetags import sizeformat
from horizon.utils import validators
from horizon import tables
from horizon import messages
from horizon import exceptions
from horizon import workflows
from horizon import forms

from openstack_dashboard.dashboards.project.instances.workflows import SetInstanceDetailsAction,KEYPAIR_IMPORT_URL
from openstack_dashboard.dashboards.project.instances.workflows import LaunchInstance, SetInstanceDetails, SetNetwork
from openstack_dashboard.dashboards.project.images_and_snapshots.utils import get_available_images
from openstack_dashboard.dashboards.project.instances.tables import  POWER_STATES,TerminateInstance,SimpleAssociateIP, AssociateIP,SimpleDisassociateIP
from openstack_dashboard.dashboards.project.access_and_security.floating_ips.workflows import IPAssociationWorkflow
from openstack_dashboard.usage import quotas
from openstack_dashboard import api
from openstack_dashboard.api import network
from openstack_dashboard.api import nova


from contextlib import closing
import tempfile
import zipfile
import uuid
import json
from ...hadoop.api import hadoop

class SetSlaveDetailsAction(workflows.Action):
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
    contributes = ( "flavor", "count",)
    class Meta:
        name = _("Details")
        help_text_template = ("project/instances/"
                              "_launch_details_help.html")

class CreateSlaveWorkflow(workflows.Workflow):
    slug = "create_group"
    name = _("Create Hadoop Slave")
    finalize_button_name = _("Create")
    success_message = _('Created Hadoop Slave Success.')
    failure_message = _('Unable to Create Hadoop Slave "%s".')
    success_url = "horizon:custom:hadoop:index"
    default_steps = (
                     SetSlaveDetails,
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
    classes = ( "btn" , "btn-primary")

    def get_link_url(self, datum=None):
        return reverse('horizon:custom:hadoop:job:index', args=(self.table.kwargs["group_id"] , ))


def get_full_flavor(request,flavor_id):
    key = "flavor-%s"%(flavor_id ,)
    ret = cache.get(key,None)
    if ret:
        return ret

    full_flavor = api.nova.flavor_get(request,flavor_id)

    if full_flavor is None:
        ret = _("Not available")
        cache.set(key,ret,300)
        return ret
    size_string = _("%(name)s | %(RAM)s RAM | %(VCPU)s VCPU "
                "| %(disk)s Disk")
    vals = {'name': full_flavor.name,
        'RAM': sizeformat.mbformat(full_flavor.ram),
        'VCPU': full_flavor.vcpus,
        'disk': sizeformat.diskgbformat(full_flavor.disk)}

    ret = size_string % vals
    cache.set(key,ret,1800)
    return ret

def get_floating_ip(request ,instance_id):
    key = "floating-ips-%s"%( request.user.tenant_id , )
    return cache.get(key,{}).get(instance_id,None)

def get_floating_ips_list(request):
    try:
        floating_ips = network.tenant_floating_ip_list(request)
    except:
        floating_ips = []

    for ip in floating_ips :
        if not ip.instance_id:
            continue

    ips = dict([(ip.instance_id,ip.ip) for ip in floating_ips if ip.instance_id ])
    key = "floating-ips-%s"%( request.user.tenant_id , )
    cache.set(key,ips,300)
    return ips

class UpdateRow(tables.Row):
    ajax = True
    def get_data(self, request,instance_id):
        func, args, kwargs = resolve(request.META["PATH_INFO"])
        datum = hadoop.get_instance_meta(request,kwargs['group_id'],instance_id)
        try:
            instance = api.nova.server_get(request, instance_id)
            datum['full_flavor'] = get_full_flavor(request,instance.flavor["id"])
            datum['instance'] = instance
            datum['float_ip'] = get_floating_ip(request,instance_id)
            if datum.get('float_ip',None):
                if datum.get('HADOOP_TYPE',None) == "master":
                    datum['service_link']="""
<li><a href="http://{float_ip}:50070/">NameNode</a></li>
<li><a href="http://{float_ip}:50030/">JobTracker</a></li>
""".format(**datum)
                if datum.get('HADOOP_TYPE',None) == "slave":
                    datum['service_link']="""
<li><a href="http://{float_ip}:50075/browseDirectory.jsp?dir=%2F">DataNode</a></li>
<li><a href="http://{float_ip}:50060/">TaskTracker</a></li>
""".format(**datum)

            datum['state'] = POWER_STATES.get(getattr(instance, "OS-EXT-STS:power_state", 0), '')

        except:
            self.table.disable_column_link()
            datum['state'] = "Terminated"
        datum["request"]=request
        return datum

class _AssociateIP(AssociateIP):
    def get_link_url(self, datum):
        func, args, kwargs = resolve(self.table.request.META["PATH_INFO"])

        base_url = urlresolvers.reverse(self.url)
        next = urlresolvers.reverse("horizon:custom:hadoop:group:index",
                       args=(kwargs["group_id"],))

        params = {"instance_id": self.table.get_object_id(datum),
                  IPAssociationWorkflow.redirect_param_name: next}
        params = urlencode(params)
        return "?".join([base_url, params])

class _SimpleDisassociateIP(SimpleDisassociateIP):
    def single(self, table, request, instance_id):
        try:
            target_id = api.network.floating_ip_target_get_by_instance(
                request, instance_id).split('_')[0]

            fips = [fip for fip in api.network.tenant_floating_ip_list(request)
                    if fip.port_id == target_id]
            if fips:
                fip = fips.pop()
                api.network.floating_ip_disassociate(request,
                                                     fip.id, target_id)
                messages.success(request,
                                 _("Successfully disassociated "
                                   "floating IP: %s") % fip.ip)
            else:
                messages.info(request, _("No floating IPs to disassociate."))
        except:
            exceptions.handle(request,
                              _("Unable to disassociate floating IP."))
        func, args, kwargs = resolve(self.table.request.META["PATH_INFO"])
        url = urlresolvers.reverse("horizon:custom:hadoop:group:index",
                       args=(kwargs["group_id"],))
        return shortcuts.redirect(url)


class RebootInstance(tables.BatchAction):
    name = "reboot"
    action_present = _("Hard Reboot")
    action_past = _("Hard Rebooted")
    data_type_singular = _("Instance")
    data_type_plural = _("Instances")
    classes = ('btn-danger', 'btn-reboot')

    def allowed(self, request, instance=None):
        return True

    def action(self, request, obj_id):
        api.nova.server_reboot(request, obj_id, api.nova.REBOOT_HARD)



class Table(tables.DataTable):
    class Meta:
        name = "Hadoop Group"
        table_actions = ()
        status_columns = ["ajax_state" , ]
        table_actions = ( JobListAction, CreateSlaveAction,RebootInstance,TerminateInstance )
        row_actions = ( _AssociateIP,_SimpleDisassociateIP,RebootInstance,TerminateInstance)
        row_class = UpdateRow

    def get_ajax_state(datum):
        if datum.has_key("request"):
            return "true"
        return None
    ajax_state = tables.Column(get_ajax_state,
                        verbose_name="ajax_state",
                        status=True,
                        hidden=True,
                )

    def get_name(datum):
        return  datum.get("name",None) or datum.get("uuid",None)
    
    name = tables.Column(get_name,
                         link=("horizon:custom:hadoop:detail"),
                         verbose_name=_("Name"))
    def get_state(datum):
        return datum.get("state",None)


    def get_ips(datum):
        if not datum.get('instance',None):
            return '-'

        t=Template("""
{% for ip_group, addresses in instance.addresses.items %}
    {% if instance.addresses.items|length > 1 %}
    <h4>{{ ip_group }}</h4>
    {% endif %}
    <ul>
    {% for address in addresses %}
      <li>{{ address.addr }}</li>
    {% endfor %}
    </ul>
    {% if service_link %}
      {{ service_link | safe }}
    {% endif %}
{% endfor %}
""")
        #template_name = 'project/instances/_instance_ips.html'
        #return template.loader.render_to_string(template_name, datum)
        return t.render(Context(datum))

    def get_type(datum):
        return datum.get('HADOOP_TYPE','-')
    hadoop_type = tables.Column(get_type,
                         verbose_name=_("Type"),
                         )

    ip = tables.Column(get_ips,
                       verbose_name=_("IP Address"),
                       attrs={'data-type': "ip"})


    def get_size(datum):
        return datum.get('full_flavor','-')
    size = tables.Column(get_size,
                         verbose_name=_("Size"),
                         attrs={'data-type': 'size'})
    def get_init(datum):
        if not datum.get("instance",None):
            return "-"
        if datum.get("private_ip_address",None):
            return  "ok"
        return "installing"
        

    def disable_column_link(self):
        cols = self.columns
        for key in cols:
            cols[key].link=None

    state = tables.Column(get_state ,
                        verbose_name="State",
                )
    init = tables.Column(get_init ,
                        verbose_name="Init",
                )

    def get_object_id(self, datum):
        return datum["uuid"]
    def get_object_display(self, datum):
        return  datum.get("name",None) or datum.get("uuid",None)


class GroupView(tables.DataTableView):
    table_class = Table
    template_name = 'custom/hadoop/tables.html'
    def get_data(self):
        request = self.request
        get_floating_ips_list(self.request)
        return hadoop.get_instance_list(request,self.kwargs['group_id'])

