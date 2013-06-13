from django.utils.translation import ugettext_lazy as _

import horizon

from custom import dashboard


class Hadoop(horizon.Panel):
    name = _("Hadoop")
    slug = "hadoop"


dashboard.Custom.register(Hadoop)
