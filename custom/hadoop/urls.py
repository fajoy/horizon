from django.conf.urls.defaults import patterns, url

from .views import IndexView
from .views import CreateView


urlpatterns = patterns('',
    url(r'^$', IndexView.as_view(), name='index'),
    url(r'^create$', CreateView.as_view(), name='create'),
)

