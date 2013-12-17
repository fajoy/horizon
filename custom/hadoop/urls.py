from django.conf.urls.defaults import url, patterns, include
from openstack_dashboard.dashboards.project.instances.views import DetailView
from openstack_dashboard import api

from .index import CreateMasterView
from .index import CreateEc2KeyView
from .index import IndexView
from .group import urls as group_urls
from .index import IndexView
from .show import ShowView
from .job import urls as job_urls

urlpatterns = patterns('',
    url(r'^$', IndexView.as_view(), name='index'),
    url(r'^create$', CreateMasterView.as_view(), name='create'),
    url(r'^create_ec2$', CreateEc2KeyView.as_view() , name='create_ec2'),
    url(r'^instances/(?P<instance_id>[^/]+)/$', DetailView.as_view(), name='detail'),
    url(r'group/', include(group_urls, namespace='group')),
    url(r'job/', include(job_urls, namespace='job')),
    url(r'^show/(?P<path>.+)$', ShowView.as_view(), name='show'),
)

