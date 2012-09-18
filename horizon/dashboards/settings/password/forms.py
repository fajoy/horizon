# vim: tabstop=4 shiftwidth=4 softtabstop=4

# User's name, email, password update page
# reference from /dashboards/syspanel/users/forms.py
# by chuanyu
import logging

from django.conf import settings
from django import shortcuts
from django.contrib import messages
from django.utils.translation import force_unicode, ugettext_lazy as _
from django.forms import ValidationError


from horizon import api
from horizon import exceptions
from horizon import forms
from horizon.utils import validators
from horizon.api import base


from keystoneclient.v2_0 import client as keystone_client

LOG = logging.getLogger(__name__)

# eliminate tenant choices
class BaseUserForm(forms.SelfHandlingForm):
    def __init__(self, request, *args, **kwargs):
        super(BaseUserForm, self).__init__(*args, **kwargs)

    @classmethod
    def _instantiate(cls, request, *args, **kwargs):
        return cls(request, *args, **kwargs)

    def clean(self):
        '''Check to make sure password fields match.'''
        data = super(forms.Form, self).clean()
        if 'password' in data:
            if data['password'] != data.get('confirm_password', None):
                raise ValidationError(_('Passwords do not match.'))
        return data

# Just get identity adminURL endpoint
def _get_endpoint_url(request):
    return base.url_for(request,
                        service_type='identity',
                        endpoint_type='adminURL')

# Keystone only let admin manage his data,
# here use admin token from config
def _keystone_client_conn(request):
    _tok = getattr(settings, 'KEYSTONE_TOKEN', "")
    adminURL = _get_endpoint_url(request)
    return keystone_client.Client(username=None,password=None,
                        tenant_id=None,
                        token=_tok,
                        auth_url=adminURL,
                        endpoint=adminURL)

class UpdateUserForm(BaseUserForm):
    id = forms.CharField(label=_("ID"), widget=forms.HiddenInput)
    name = forms.CharField(label=_("User Name"))
    email = forms.EmailField(label=_("Email"))
    current_password = forms.CharField(
            label=_("Current Password"),
            widget=forms.PasswordInput(render_value=False),
            required=False)
    password = forms.RegexField(label=_("Password"),
            widget=forms.PasswordInput(render_value=False),
            regex=validators.password_validator(),
            required=False,
            error_messages={'invalid': validators.password_validator_msg()})
    confirm_password = forms.CharField(
            label=_("Confirm Password"),
            widget=forms.PasswordInput(render_value=False),
            required=False)

    def handle(self, request, data):
        failed, succeeded = [], []

        user = data.pop('id')
        password = data.pop('password')
        current_password = data.pop('current_password')
        # Discard the extra fields so we can pass kwargs to keystoneclient
        data.pop('method')
        data.pop('confirm_password', None)

        # Update user email
        msg_bits = (_('name'), _('email'))
        try:
            _keystone_client_conn(
                    request).users.update(user, **data)
            succeeded.extend(msg_bits)
        except:
            failed.extend(msg_bits)
            exceptions.handle(request, ignore=True)

        # If password present , update
        if password:
            msg_bits = (_('password'),)
            try:
                # check current_password first
                try:
                    api.token_create(request, 
                            tenant=None,
                            username=data["name"],
                            password=current_password)
                except:
                    messages.error(request,
                            _('Old Password Mismatch, Are You "%(user)s"?')
                            % {"user": data["name"]})
                else:
                    # starting update passowrd
                    _keystone_client_conn(
                            request).users.update_password(user, password)
                    succeeded.extend(msg_bits)
            except:
                failed.extend(msg_bits)
                exceptions.handle(request, ignore=True)


        if succeeded:
            succeeded = map(force_unicode, succeeded)
            messages.success(request,
                             _('Updated %(attributes)s for "%(user)s".')
                               % {"user": data["name"],
                                  "attributes": ", ".join(succeeded)})
        if failed:
            failed = map(force_unicode, failed)
            messages.error(request,
                           _('Unable to update %(attributes)s for "%(user)s".')
                             % {"user": data["name"],
                                "attributes": ", ".join(failed)})
        return shortcuts.redirect('horizon:settings:password:index')

