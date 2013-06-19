from django.conf.urls.defaults import patterns, url

from .views import IndexView
from .views import ShowScriptView
from .views import EditScriptView
from .views import CreateScriptView


urlpatterns = patterns('',
    url(r'^$', IndexView.as_view(), name='index'),
    url(r'^script/(?P<name>[^/]+)/show$', ShowScriptView.as_view(), name='show_script'),
    url(r'^script/create$', CreateScriptView.as_view(), name='create_script'),
    url(r'^script/(?P<name>[^/]+)/edit$', EditScriptView.as_view(), name='edit_script'),
)
