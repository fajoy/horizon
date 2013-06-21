from django.utils.translation import ugettext_lazy as _

import horizon


class Custom(horizon.Dashboard):
    name = _("Custom")
    slug = "custom"
    panels = ('ec2' , 'config' )   
    default_panel = 'ec2'  
    supports_tenants = True

horizon.register(Custom)
