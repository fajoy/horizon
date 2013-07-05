from django.conf.urls.defaults import patterns, url

from .views import IndexView
from .views import GroupView
from .views import CreateView
from openstack_dashboard.dashboards.project.instances.views import DetailView


urlpatterns = patterns('',
    url(r'^$', IndexView.as_view(), name='index'),
    url(r'^create$', CreateView.as_view(), name='create'),
    url(r'^group/(?P<group_id>[^/]+)/$', GroupView.as_view(), name='group'),
    url(r'^instances/(?P<instance_id>[^/]+)/$', DetailView.as_view(), name='detail'),
)

