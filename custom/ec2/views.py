from horizon import views
from django.utils.translation import ugettext_lazy as _
from django import shortcuts

from openstack_dashboard import api
from openstack_dashboard.api.keystone import keystoneclient
from horizon import tables
from horizon import messages

class CreateEC2Key(tables.Action):
    name = "Create"
    verbose_name = "Create EC2 Key"
    requires_input=False
    classes = ("btn-create" ,)

    def single(self, data_table, request, object_ids):
        self.success_url=request.get_full_path()+"test"
        user_id=request.user.id
        tenant_id=request.user.tenant_id
        try:
            ec2=api.keystone.create_ec2_credentials(request,user_id,tenant_id)
            messages.success(request,"Create Success : %s"%ec2.access)
            return shortcuts.redirect(self.get_success_url(request))
        except:
            messages.error(request,"Create Error")

    def get_success_url(self, request=None):
        """
        Returns the URL to redirect to after a successful action.
        """
        return request.get_full_path()

class DeleteEC2Key(tables.DeleteAction):
    data_type_singular = _("EC2 Key")
    data_type_plural = _("EC2 Key")

    def delete(self, request, obj_id):
        access=obj_id
        user_id=request.user.id

        try:
            keystoneclient(request).ec2.delete(user_id,access)
        except:
            messages.error(request,"Create Error")

class _Table(tables.DataTable):
    access = tables.Column("access")
    secret = tables.Column("secret")

    def get_object_id(self, datum):
        return datum.access
    def get_object_display(self, datum):
        return datum.access

    class Meta:
        name = "EC2 Key"
        table_actions = (CreateEC2Key,DeleteEC2Key,)
        row_actions = (DeleteEC2Key,)

class IndexView(tables.DataTableView):
    template_name = 'custom/ec2/tables.html'
    table_class = _Table
    def get_data(self):
        return api.keystone.list_ec2_credentials(self.request,self.request.user.id)
