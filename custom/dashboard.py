from django.utils.translation import ugettext_lazy as _

import horizon


class Custom(horizon.Dashboard):
    slug = "custom"
    name = _("NCTU")
    panels = ('ec2','hadoop', )   
    default_panel = 'ec2'
    supports_tenants = True

horizon.register(Custom)
