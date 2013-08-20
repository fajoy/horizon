from django.utils.translation import ugettext_lazy as _

import horizon


class Custom(horizon.Dashboard):
    name = _("NCTU")
    slug = "custom"
    panels = ('ec2', 'config', 'hadoop', )   
    default_panel = 'ec2'
    supports_tenants = True

horizon.register(Custom)
