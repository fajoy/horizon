from django.utils.translation import ugettext_lazy as _
from django.utils.text import normalize_newlines
from django.utils.html import escape
from django.core.urlresolvers import reverse
from django.core.urlresolvers import resolve
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

    log_update_sec = forms.IntegerField(label=_("Update interval"),
                               min_value=0,
                               initial=3,
                               help_text=_('Stdout and Stderr update interval.("0" is when proccess finished updating.)') )


    def __init__(self, request, context, *args, **kwargs):
        super(JobAction, self).__init__(request, context, *args, **kwargs)
        if context.get("hadoop_group_id",None):
            self.fields["hadoop_group_id"].widget.attrs={'readonly': 'readonly'}

    def handle(self, request, data):
        return True

    def get_help_text(self, extra_context=None):
        return """Input job name and Stdout amd Stderr update interval seccond second."""

        
class JobStep(workflows.Step):
    slug="job"
    action_class = JobAction
    contributes = ("hadoop_group_id","job_name","log_update_sec")
    def contribute(self, data, context):
        context.update(data)
        return context

class ScriptAction(workflows.Action):
    class Meta:
        name = "Script"

    script =  forms.Field( widget=forms.Textarea({'style':'margin: 0px 0px 0px; width:630px; height: 200px;' }),
                            label=_("Script"),
                            required=True,
                            )
    def __init__(self, request, context, *args, **kwargs):
        super(ScriptAction, self).__init__(request, context, *args, **kwargs)
        self.fields["script"].initial=context.get("script","""""")

    def handle(self, request, data):
        return True

    def get_help_text(self, extra_context=None):
        return """
<ul class="nav nav-tabs">
        <li class="active">
          <a href="#ex1action" data-toggle="tab" data-target="#ex1action">Put File</a>
        </li>
        <li class="">
          <a href="#ex2action" data-toggle="tab" data-target="#ex2action">WordCount</a>
        </li>
        <li class="">
          <a href="#ex3action" data-toggle="tab" data-target="#ex3action">TeraSort</a>
        </li>
        <li class="">
          <a href="#ex4action" data-toggle="tab" data-target="#ex4action">Streaming</a>
        </li>
        <li class="">
          <a href="#ex4action" data-toggle="tab" data-target="#ex5action">Boto</a>
        </li>
</ul>
<div class="tab-content dropdown_fix">
<fieldset id="ex1action" class="js-tab-pane tab-pane active">
Put file example
<a id="add_script" class="btn add_script">copy</a>
<pre id="ex1">
export bucket="&lt;bucket_name&gt;"   #change you bucket name 
apt-get install -y wget unzip
wget http://www.java2s.com/Code/JarDownload/hadoop/hadoop-examples.jar.zip
unzip hadoop-examples.jar.zip
hadoop fs -put hadoop-examples.jar s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@${bucket}/hadoop-examples.jar 
wget http://www.java2s.com/Code/JarDownload/hadoop/hadoop-streaming.jar.zip
unzip hadoop-streaming.jar.zip
hadoop fs -put hadoop-streaming.jar s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@${bucket}/hadoop-streaming.jar
</pre>
</fieldset>
         
<fieldset id="ex2action" class="js-tab-pane tab-pane">
WordCount example
<a id="add_script" class="btn add_script">copy</a>
<pre>
export bucket="&lt;bucket_name&gt;"   #change you bucket name 
export jar_location="s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@${bucket}/hadoop-examples.jar"
export jar=`basename ${jar_location}`
export rtw_out="s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@${bucket}/rtw"
export wc_out="s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@${bucket}/rtw_wc"
export size="1024"
export map_count="1"
export reduce_count="1"
hadoop fs -get ${jar_location} ${jar}
hadoop jar ${jar} randomtextwriter -D test.randomtextwrite.bytes_per_map=$((${size}/${map_count})) -D test.randomtextwrite.total_bytes=${size} -outFormat org.apache.hadoop.mapred.TextOutputFormat ${rtw_out}
hadoop job -history all ${rtw_out}
hadoop jar ${jar} wordcount -D mapred.reduce.tasks=${reduce_count} ${rtw_out} ${wc_out}
hadoop job -history all ${wc_out}
</pre>
</fieldset>
 
<fieldset id="ex3action" class="js-tab-pane tab-pane">
TeraSort example
<a id="add_script" class="btn add_script">copy</a>
<pre>
export bucket="&lt;bucket_name&gt;"   #change you bucket name 
export jar_location="s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@${bucket}/hadoop-examples.jar"
export jar=`basename ${jar_location}`
export teragen_out="s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@${bucket}/teragen"
export terasort_out="s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@${bucket}/terasort"
export size="1024"
export map_count="1"
export reduce_count="1"
export task_max_ram="200m"
export timeout="600000"
export sample_size="100000"
hadoop fs -get ${jar_location} ${jar}
hadoop jar ${jar} teragen -D mapred.task.timeout=${timeout} -D mapred.child.java.opts=-Xmx${task_max_ram} -D mapred.map.tasks=${map_count} $((${size}/100)) ${teragen_out}
hadoop job -history all ${teragen_out}
hadoop jar ${jar} terasort -D mapred.task.timeout=${timeout} -D mapred.child.java.opts=-Xmx${task_max_ram} -D mapred.map.tasks=${map_count} -D mapred.reduce.tasks=${reduce_count} -D terasort.partitions.sample=${sample_size} ${teragen_out} ${terasort_out}
hadoop job -history all ${terasort_out}
</pre>
</fieldset>
 
<fieldset id="ex4action" class="js-tab-pane tab-pane">
Hadoop Stream example
<a id="add_script" class="btn add_script">copy</a>
<pre>
export bucket="&lt;bucket_name&gt;"   #change you bucket name 
export jar_location="s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@${bucket}/hadoop-streaming.jar"
export jar=`basename ${jar_location}`
export input="s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@${bucket}/input"
export output="s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@${bucket}/output"
export mapper="/bin/cat"
export reducer="/usr/bin/wc"
export reduce_count="1"
hadoop fs -get ${jar_location} ${jar}
hadoop jar ${jar} -input ${input} -output ${output} -mapper ${mapper} -reducer ${reducer} -numReduceTasks ${reduce_count}
hadoop job -history all ${output}
</pre>
</fieldset>

<fieldset id="ex5action" class="js-tab-pane tab-pane">
Boto example
<a id="add_script" class="btn add_script">copy</a>
<pre>
#!/usr/bin/env python
#ref https://github.com/boto/boto/blob/develop/docs/source/s3_tut.rst
import boto
from boto.s3.key import Key
bucket_name="" #input bucket name
s3 = boto.connect_s3()
bucket = s3.lookup(bucket_name)
if bucket is None: #bucket not exist
    bucket = s3.create_bucket(bucket_name) #create bucket
key = Key(bucket,"hello/hello.txt") #set key
key.set_contents_from_string("hello world this is boto.") #put object
key = bucket.lookup("hello/hello.txt") #get key
print key.get_contents_as_string(headers={'Range' : 'bytes=0-11'}) #get object
</pre>
</fieldset>

<script>
$(".add_script").click(function() {
$("#id_script" ).val($("#id_script" ).val()+$(this).next("pre").text());
});
</script>
"""
 

class ScriptStep(workflows.Step):
    slug="Script"
    action_class = ScriptAction
    contributes = ("script" , )
    template_name = "custom/hadoop/_workflow_step_horizontal.html"
    def contribute(self, data, context):
        context.update(data)
        return context


class CreateScriptWorkflow(workflows.Workflow):
    slug = "CreateScriptWorkflow"
    name = "Create Script Job"
    finalize_button_name = "Save"
    success_message = 'success_message "{job_name}".'
    failure_message = 'failure_message "{job_name}".'
    default_steps = (JobStep ,
                     ScriptStep,)

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
        context["script"]=normalize_newlines(context["script"])
        return hadoop.create_script(request,context)

class CreateScriptView(workflows.WorkflowView):
    workflow_class = CreateScriptWorkflow

    def get_initial(self):
        initial=super(CreateScriptView, self).get_initial()
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
                     help_text=_("ex: {bucket}/hadoop-example.jar"), )

    jar_args =  forms.Field( widget=forms.Textarea({'style':'margin: 0px 0px 0px; height: 300px;' }),
                            label=_("JAR arguments"),
                            required=True,
                            )

    def handle(self, request, data):
        return True

    def get_help_text(self, extra_context=None):
        return """
JAR location example: 
<pre>
&lt;bucket_name&gt;/hadoop-examples.jar
</pre>
JAR arguments example:
<pre>
wordcount 
s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@&lt;bucket_name&gt;/&lt;input_path&gt;
s3n://$EC2_ACCESS_KEY:$EC2_SECRET_KEY@&lt;bucket_name&gt;/&lt;output_path&gt;
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
        context["jar_args"]=" ".join(context["jar_args"].split())
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
                     help_text=escape("ex: &lt;bucket_name&gt;/input", ) )

    output_location = forms.Field(label=("Output location"),
                     required=True,
                     help_text=escape("ex: $lt;bucket_name&gt;/output", ) )


    mapper = forms.Field(label=("Mapper"),
                     required=True,
                     help_text=escape("ex: &lt;bucket_name&gt;/mapper.py"), )


    reducer = forms.Field(label=("Reducer"),
                     required=True,
                     help_text=escape("ex: &lt;bucket_name&gt;/reducer.py"), )

    extea_args = forms.Field( widget=forms.Textarea({'style':'margin: 0px 0px 0px; height: 300px;' }),
                            label=_("Extea Arguments (Option)"),
                            required=False,
                            help_text=escape("ex: -numReduceTasks [num] ... etc."), )

    def handle(self, request, data):
        return True

    def get_help_text(self, extra_context=None):
        return """
Input location exmple: 
<pre>
&lt;bucket_name&gt;/input
</pre>
Output location exmple:
<pre>
&lt;bucket_name&gt;/output
</pre>
Mapper example: 
<pre>
&lt;bucket_name&gt;/string_tokenizer.py 
</pre>
Reducer example:
<pre>
&lt;bucket_name&gt;/count.py 
</pre>
Extea Arguments example (Option):
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
        context["extea_args"]=" ".join(context["extea_args"].split())
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
    classes = ( "btn" , "btn-primary" )

    def get_link_url(self, datum=None):
        return reverse('horizon:custom:hadoop:group:index', args=(self.table.kwargs["group_id"] , ))

class CreateScriptAction(tables.LinkAction):
    name = "create_bash"
    verbose_name = "Create Script Job"
    classes = ("ajax-modal", "btn-create")

    def get_link_url(self, datum=None):
        url = "horizon:custom:hadoop:job:create_script"
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
        table_actions = ( GroupListAction ,CreateScriptAction ,CreateJarAction ,CreateStreamingAction, DeleteJobAction)
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
        return datum.get("script_type",None) or datum.get("job_type",None) or "-"
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

