from django.conf.urls.defaults import patterns, url
from django.core.urlresolvers import reverse
from django.http import HttpResponseRedirect

from .index import GroupView
from .index import CreateSlaveView

def empty(reqeust):
    return HttpResponseRedirect(reverse('horizon:custom:hadoop:index', args=()))

urlpatterns = patterns('',
    url(r'^$',empty , name='empty'),
    url(r'^(?P<group_id>[^/]+)/$', GroupView.as_view(), name='index'),
    url(r'^(?P<group_id>[^/]+)/create_slave$', CreateSlaveView.as_view(), name='create_slave'),
)

