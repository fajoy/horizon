# vim: tabstop=4 shiftwidth=4 softtabstop=4

# User's name, email, password update page
# reference from /dashboards/syspanel/users/view.py
# by chuanyu

from django import shortcuts

from django.conf import settings
from django.core.urlresolvers import reverse
from django.utils.translation import ugettext_lazy as _

from .forms import UpdateUserForm, _keystone_client_conn

from horizon import exceptions
from horizon import forms


def index(request):
    return shortcuts.render(request, 'settings/password/settings.html', {'user_id':request.session['user_id']})

class UpdateView(forms.ModalFormView):
    form_class = UpdateUserForm
    template_name = 'settings/password/passwd.html'
    context_object_name = 'user'

    def get_context_data(self, **kwargs):
        context = super(UpdateView, self).get_context_data(**kwargs)
        try:
            context.setdefault('user_id', kwargs['user_id']) 
        except:
            exceptions.handle(self.request)
        return context

    def get_object(self, *args, **kwargs):
        user_id = kwargs['user_id']

        try:
            return _keystone_client_conn(self.request).users.get(user_id)
        except:
            redirect = reverse("horizon:settings:password:index")
            exceptions.handle(self.request,
                              _('Unable to update user.'),
                              redirect=redirect)

    def get_initial(self):
        return {'id': self.object.id,
                'name': getattr(self.object, 'name', None),
                'email': getattr(self.object, 'email', '')}

