from django.conf.urls.defaults import patterns, url
from django.core.urlresolvers import reverse
from django.http import HttpResponseRedirect

from .index import JobIndexView
from .index import CreateScriptView
from .index import CreateJarView
from .index import CreateStreamingView

def empty(reqeust):
    return HttpResponseRedirect(reverse('horizon:custom:hadoop:index', args=()))

urlpatterns = patterns('',
    url(r'^$',empty , name='empty'),
    url(r'^(?P<group_id>[^/]+)/$', JobIndexView.as_view(), name='index'),
    url(r'^(?P<group_id>[^/]+)/create_bash$', CreateScriptView.as_view(), name='create_bash'),
    url(r'^(?P<group_id>[^/]+)/create_script$', CreateScriptView.as_view(), name='create_script'),
    url(r'^(?P<group_id>[^/]+)/create_jar$', CreateJarView.as_view(), name='create_jar'),
    url(r'^(?P<group_id>[^/]+)/create_streaming$', CreateStreamingView.as_view(), name='create_streaming'),
)

