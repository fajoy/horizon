from django.conf.urls.defaults import patterns, url

from .views import IndexView
from .views import LaunchInstanceView
from openstack_dashboard.dashboards.project.instances.views import DetailView
urlpatterns = patterns('',
    url(r'^$', IndexView.as_view(), name='index'),
    url(r'^instances/(?P<instance_id>[^/]+)/$', DetailView.as_view(), name='detail'),
)

