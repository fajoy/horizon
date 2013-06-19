from django.utils.translation import ugettext_lazy as _

import horizon

from custom import dashboard


class Config(horizon.Panel):
    name = _("Config")
    slug = "config"


dashboard.Custom.register(Config)
