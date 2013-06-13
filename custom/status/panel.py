from django.utils.translation import ugettext_lazy as _

import horizon

from custom import dashboard


class Status(horizon.Panel):
    name = _("Status")
    slug = "status"


dashboard.Custom.register(Status)
