from django.utils.translation import ugettext_lazy as _

import horizon

from custom import dashboard


class EC2(horizon.Panel):
    name = _("EC2 Key")
    slug = "ec2"


dashboard.Custom.register(EC2)
