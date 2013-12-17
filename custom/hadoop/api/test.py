#!/usr/bin/python
import os
print os.path.dirname(__file__)
print os.path.abspath(os.path.dirname(__file__))
print os.path.abspath(os.path.join(os.path.dirname(__file__),"../templates/hadoop/cloud-config/"))

