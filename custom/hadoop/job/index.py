from django.utils.translation import ugettext_lazy as _
from django.core.urlresolvers import reverse
from django.core.urlresolvers import resolve
from django.utils.html import escape
from django import shortcuts
from django.http import QueryDict

from horizon import exceptions
from horizon import tables
from horizon import messages
from horizon import workflows
from horizon import forms
from openstack_dashboard import api
from openstack_dashboard.dashboards.project.instances.tables import POWER_STATES

import uuid
import json
from ...hadoop.api import hadoop

class JobAction(workflows.Action):
    class Meta:
        name = "Job"

    hadoop_group_id = forms.Field(label=("hadoop group"),
                     required=True,
                     help_text="hadoop group", 
                     widget=forms.TextInput() )
    job_name = forms.Field(label=("job name"),
                     required=False,
                     help_text="Hadoop job name.", )
    def __init__(self, request, context, *args, **kwargs):
        super(JobAction, self).__init__(request, context, *args, **kwargs)
        if context.get("hadoop_group_id",None):
            self.fields["hadoop_group_id"].widget.attrs={'readonly': 'readonly'}

    def handle(self, request, data):
        return True

    def get_help_text(self, extra_context=None):
        return "Input job name."

        
class JobStep(workflows.Step):
    slug="job"
    action_class = JobAction
    contributes = ("hadoop_group_id","job_name")

    def contribute(self, data, context):
        context.update(data)
        return context

class BashAction(workflows.Action):
    class Meta:
        name = "Bash"

    script =  forms.Field( widget=forms.Textarea({'style':'margin: 0px 0px 0px; height: 500px;' }),
                            label=_("Bash Script"),
                            required=True,
                            )
    def __init__(self, request, context, *args, **kwargs):
        super(BashAction, self).__init__(request, context, *args, **kwargs)
        self.fields["script"].initial=context.get("script","""echo `date '+%Y/%m/%d %H:%M:%S'` Start.
#Script Code

echo `date '+%Y/%m/%d %H:%M:%S'` End.""")

    def handle(self, request, data):
        return True

    def get_help_text(self, extra_context=None):
        return """
"""
 

class BashStep(workflows.Step):
    slug="bash"
    action_class = BashAction
    contributes = ("script" , )

    def contribute(self, data, context):
        context.update(data)
        return context


class CreateBashWorkflow(workflows.Workflow):
    slug = "CreateBashWorkflow"
    name = "Create Bash Job"
    finalize_button_name = "Save"
    success_message = 'success_message "{job_name}".'
    failure_message = 'failure_message "{job_name}".'
    default_steps = (JobStep ,
                     BashStep,)

    def get_link_url(self, datum=None):
        url="horzion:custom:hadoop:job:index"
        return reverse(url, args=(self.workflow.kwargs["hadoop_group_id"] ,))

    def format_status_message(self, message):
        name = self.context.get('job_name') 
        return message % name

    def get_success_url(self):
        return reverse("horizon:custom:hadoop:job:index",
                       args=(self.context.get("hadoop_group_id"),))

    def get_failure_url(self):
        return reverse("horizon:custom:hadoop:job:index",
                       args=(self.context.get("hadoop_group_id"),))

    def format_status_message(self, message):
        return message.format(**self.context)

    def handle(self, request, context):
        return hadoop.create_bash(request,context)

class CreateBashView(workflows.WorkflowView):
    workflow_class = CreateBashWorkflow

    def get_initial(self):
        initial=super(CreateBashView, self).get_initial()
        if self.kwargs.has_key('group_id'):
            initial.update({'hadoop_group_id': self.kwargs['group_id'],})
        job_id=self.request.GET.get("job_id",None)
        if job_id:
            datum = hadoop.get_job_obj(self.request,self.kwargs["group_id"],job_id)
            initial.update(datum)
        return initial

class JarArgsAction(workflows.Action):
    class Meta:
        name = "JarArgs"

    jar_location = forms.Field(label=("JAR location"),
                     required=True,
                     help_text=_("ex: [bucket]/hadoop-example.jar"), )

    jar_args =  forms.Field( widget=forms.Textarea({'style':'margin: 0px 0px 0px; height: 300px;' }),
                            label=_("JAR arguments"),
                            required=True,
                            )

    def handle(self, request, data):
        return True

    def get_help_text(self, extra_context=None):
        return """
JAR location: 
<pre>
[bucket]/hadoop-examples.jar
</pre>
JAR arguments:
<pre>
wordcount 
s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@[bucket]/[input_path] 
s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@[bucket]/[output_path]
</pre>
"""

class JarArgsStep(workflows.Step):
    slug="job_args"
    action_class = JarArgsAction
    contributes = ("jar_location" ,"jar_args")

    def contribute(self, data, context):
        context.update(data)
        return context


class CreateJarWorkflow(workflows.Workflow):
    slug = "CreateJarWorkflow"
    name = "Create Jar Job"
    finalize_button_name = "Save"
    success_message = 'success_message "{job_name}".'
    failure_message = 'failure_message "{job_name}".'
    default_steps = (JobStep ,
                     JarArgsStep,)

    def get_link_url(self, datum=None):
        url="horzion:custom:hadoop:job:index"
        return reverse(url, args=(self.workflow.kwargs["hadoop_group_id"] ,))

    def format_status_message(self, message):
        name = self.context.get('job_name') 
        return message % name

    def get_success_url(self):
        return reverse("horizon:custom:hadoop:job:index",
                       args=(self.context.get("hadoop_group_id"),))

    def get_failure_url(self):
        return reverse("horizon:custom:hadoop:job:index",
                       args=(self.context.get("hadoop_group_id"),))

    def format_status_message(self, message):
        return message.format(**self.context)

    def handle(self, request, context):
        return hadoop.create_jar(request,context)

class CreateJarView(workflows.WorkflowView):
    workflow_class = CreateJarWorkflow

    def get_initial(self):
        initial=super(CreateJarView, self).get_initial()
        if self.kwargs.has_key('group_id'):
            initial.update({'hadoop_group_id': self.kwargs['group_id'],})
        job_id=self.request.GET.get("job_id",None)
        if job_id:
            datum = hadoop.get_job_obj(self.request,self.kwargs["group_id"],job_id)
            initial.update(datum)
        return initial
    
class StreamingArgsAction(workflows.Action):
    class Meta:
        name = "Streaming"

    input_location = forms.Field(label=("Input location"),
                     required=True,
                     help_text=escape("ex: [bucket]/[input_path]", ) )

    output_location = forms.Field(label=("Output location"),
                     required=True,
                     help_text=escape("ex: [bucket]/[output_path]", ) )


    mapper = forms.Field(label=("Mapper"),
                     required=True,
                     help_text=escape("ex: [bucket]/mapper.py"), )


    reducer = forms.Field(label=("Reducer"),
                     required=True,
                     help_text=escape("ex: [bucket]/reducer.py"), )

    extea_args = forms.Field( widget=forms.Textarea({'style':'margin: 0px 0px 0px; height: 300px;' }),
                            label=_("Extea Arguments (Option)"),
                            required=False,
                            help_text=escape("ex: -numReduceTasks [num] ... etc."), )

    def handle(self, request, data):
        return True

    def get_help_text(self, extra_context=None):
        return """
Input location: 
<pre>
[bucket]/[input_path]
</pre>
Output location:
<pre>
[bucket]/[output_path]
</pre>
Mapper: 
<pre>
[bucket]/string_tokenizer.py 
</pre>
Reducer:
<pre>
[bucket]/count.py 
</pre>
Extea Arguments (Option):
<pre>
-numReduceTasks [num]  ...etc.
</pre>
"""
        
class StreamingArgsStep(workflows.Step):
    slug="Streaming"
    action_class = StreamingArgsAction
    contributes = ( 
                    "input_location" ,
                    "output_localtion" ,
                    "mapper" ,
                    "reducer" ,
                    "other_location" ,
                    "extea_args" ,
                    )

    def contribute(self, data, context):
        context.update(data)
        return context

class CreateStreamingWorkflow(workflows.Workflow):
    slug = "CreateStreamingWorkflow"
    name = "Create Streaming Job"
    finalize_button_name = "Save"
    success_message = 'success_message "{job_name}".'
    failure_message = 'failure_message "{job_name}".'
    default_steps = (JobStep ,
                     StreamingArgsStep,
                    )

    def get_link_url(self, datum=None):
        url="horzion:custom:hadoop:job:index"
        return reverse(url, args=(self.workflow.kwargs["hadoop_group_id"] ,))

    def format_status_message(self, message):
        name = self.context.get('job_name') 
        return message % name

    def get_success_url(self):
        return reverse("horizon:custom:hadoop:job:index",
                       args=(self.context.get("hadoop_group_id"),))

    def get_failure_url(self):
        return reverse("horizon:custom:hadoop:job:index",
                       args=(self.context.get("hadoop_group_id"),))

    def format_status_message(self, message):
        return message.format(**self.context)

    def handle(self, request, context):
        return hadoop.create_streaming(request,context)

class CreateStreamingView(workflows.WorkflowView):
    workflow_class = CreateStreamingWorkflow

    def get_initial(self):
        initial=super(CreateStreamingView, self).get_initial()
        if self.kwargs.has_key('group_id'):
            initial.update({'hadoop_group_id': self.kwargs['group_id'],})
        job_id=self.request.GET.get("job_id",None)
        if job_id:
            datum = hadoop.get_job_obj(self.request,self.kwargs["group_id"],job_id)
            initial.update(datum)
        return initial
 
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
        datum["request"]=request
        return datum

class GroupListAction(tables.LinkAction):
    name = "group_list"
    verbose_name = "Instance List"
    classes = ( "btn" , )

    def get_link_url(self, datum=None):
        return reverse('horizon:custom:hadoop:group:index', args=(self.table.kwargs["group_id"] , ))

class CreateBashAction(tables.LinkAction):
    name = "create_bash"
    verbose_name = "Create Bash Job"
    classes = ("ajax-modal", "btn-create")

    def get_link_url(self, datum=None):
        url = "horizon:custom:hadoop:job:create_bash"
        return reverse(url, args=(self.table.kwargs["group_id"] ,))

class CreateJarAction(tables.LinkAction):
    name = "create_jar"
    verbose_name = "Create Jar Job"
    classes = ("ajax-modal", "btn-create")

    def get_link_url(self, datum=None):
        url = "horizon:custom:hadoop:job:create_jar"
        return reverse(url, args=(self.table.kwargs["group_id"] ,))

class CreateStreamingAction(tables.LinkAction):
    name = "create_streaming"
    verbose_name = "Create Streaming Job"
    classes = ("ajax-modal", "btn-create")

    def get_link_url(self, datum=None):
        if self.table.kwargs.has_key("group_id"):
            url = "horizon:custom:hadoop:job:create_streaming"
            return reverse(url, args=(self.table.kwargs["group_id"] ,))
        url = "horizon:custom:hadoop:job:create_streaming"
        return reverse(url, args=())

class DeleteJobAction(tables.BatchAction):
    name = "Delete Job"
    action_present = _("Delete")
    action_past = _("Delete of")
    data_type_singular = _("Job")
    data_type_plural = _("Job")
    classes = ('btn-danger', 'btn-terminate')
    def __init__(self):
        super(DeleteJobAction, self).__init__()

    def allowed(self, request, datum=None):
        return True

    def action(self, request,job_id):
        func, args, kwargs = resolve(request.META["PATH_INFO"])
        group_id=kwargs['group_id']
        hadoop.delete_job(request,group_id,job_id)
        return shortcuts.redirect(self.get_success_url(request))

    def get_success_url(self, request=None):
        return request.get_full_path()

class UpdateRow(tables.Row):
    ajax = True
    def get_data(self, request,id):
        func, args, kwargs = resolve(request.META["PATH_INFO"])
        datum = hadoop.get_job_obj(request,kwargs["group_id"],id)
        datum["request"]= request
        url="horizon:custom:hadoop:show"

        path='job/'+self.table.kwargs["group_id"]+"/"+id
        link = reverse(url, args=(path ,))
        self.table.set_column_link("output_path",link)

        path='job/'+self.table.kwargs["group_id"]+"/"+id+"/"+"log/stdout"
        link = reverse(url, args=(path ,))
        self.table.set_column_link("stdout",link)

        path='job/' +self.table.kwargs["group_id"]+"/"+id+"/"+"log/stderr"
        link = reverse(url, args=(path ,))
        self.table.set_column_link("stderr",link)
        job_type=datum.get("job_type",None)
        if job_type:
            datum["clone"]="clone"
            url="horizon:custom:hadoop:job:create_"+job_type
            qs=QueryDict('').copy()
            qs.update({"job_id":id})
            link = "%s?%s" %(reverse(url, args=(self.table.kwargs["group_id"] , )),qs.urlencode())
            self.table.set_column_link("clone",link)

        return datum

class Table(tables.DataTable):
    class Meta:
        name = "Hadoop Job"
        status_columns = ["ajax_state" , ]
        table_actions = ( GroupListAction ,CreateBashAction ,CreateJarAction ,CreateStreamingAction, DeleteJobAction)
        row_class = UpdateRow 
    def get_name(datum):
        return datum.get("name",None) or datum.get("job_name",None) 

    name = tables.Column(get_name,
                        verbose_name=_("Job Name"),
                        )

    def set_column_link(self,column_name,link):
        cols = self.columns
        if cols.has_key(column_name):
            cols[column_name].link=link

    def get_type(datum):
        return datum.get("job_type",None) or "-"
    job_type = tables.Column(get_type,
                        verbose_name=_("Job Type"),
                        link_classes= ( "ajax-modal","btn")
                        )

    def get_json(datum):
        return "json"
    output_path = tables.Column(get_json,
                        verbose_name=_("Job Json"),
                        link_classes= ( "ajax-modal","btn")
                        )

    def get_stdout(datum):
        return "stdout"
    stdout = tables.Column(get_stdout,
                        verbose_name=_("Stdout"),
                        link_classes= ( "ajax-modal", "btn")
                        )


    def get_stderr(datum):
        return "stderr"
    stderr = tables.Column(get_stderr,
                        verbose_name=_("Stderr"),
                        link_classes= ( "ajax-modal", "btn" )
                        )

    def get_clone(datum):
        return datum.get("clone",None)
    clone = tables.Column(get_clone,
                        verbose_name=_("Clone"),
                        link_classes= ( "ajax-modal","btn")
                        )



    def get_ajax_state(datum):
        if datum.has_key("request"):
            return "true"
        return None
    ajax_state = tables.Column(get_ajax_state ,
                        verbose_name="ajax_state",
                        status=True,
                        hidden=True,
                )

    def get_object_id(self, datum):
        return datum["job_id"]
    def get_object_display(self, datum):
        return datum.get("name",None) or datum.get("job_name",None)

class JobIndexView(tables.DataTableView):
    table_class = Table
    template_name = 'custom/hadoop/tables.html'
    def get_data(self):
        return hadoop.get_job_list(self.request,self.kwargs['group_id'])

