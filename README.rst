=============================
開發筆記
=============================
* Ubuntu 12.04套件需求 ::

    apt-get install git gcc python-virtualenv  python-dev nodejs libxml2-dev libxslt-dev

* 下載安裝 ::

    git clone https://github.com/fajoy/horizon.git    
    cd horizon
    cp openstack_dashboard/local/local_settings.py.example openstack_dashboard/local/local_settings.py
    ./run_tests.sh
    tools/with_venv.sh pip install --upgrade -r /var/www/horizon/requirements.txt


目前因有修改swift api 的部分,因此會有test error出現,但不影響執行運作.

* 加入模組

編輯 openstack_dashboard/local/local_settings.py ::

    CUSTOM_HADOOP_IMAGE_LIST=["d759863b-c219-4513-8963-b98dc055177f" ,]
    CUSTOM_HADOOP_S3_HOST = "s3.nctu.edu.tw"
    OPENSTACK_HOST = "openstack-grizzly.it.nctu.edu.tw"
    INSTALLED_APPS = (
        'openstack_dashboard',
        'django.contrib.contenttypes',
        'django.contrib.auth',
        'django.contrib.sessions',
        'django.contrib.messages',
        'django.contrib.staticfiles',
        'django.contrib.humanize',
        'compressor',
        'horizon',
        'openstack_dashboard.dashboards.project',
        'openstack_dashboard.dashboards.admin',
        'openstack_dashboard.dashboards.settings',
        'openstack_auth',
        'custom',
    )
    
* 不需DEBUG 可修改為 ::

    DEBUG = False
    DEBUG404 = False
    TEMPLATE_DEBUG = False
    PROD = True
    USE_SSL = False


* 啟動測試 ::

    ./run_tests.sh --runserver 0.0.0.0:8000


* 變更Session有效時間(需與keystone token有效時間相同) ::
    #session time 2 hour
    SESSION_COOKIE_AGE = 7200

* Apache WSGI Module 設定範例 ::

    <VirtualHost *:80>
      ServerAdmin <ServerAdmin@mail.com>
      ServerName <ServerName>
    
      LogLevel warn
      ErrorLog  ${APACHE_LOG_DIR}/horizon.error.log
      CustomLog ${APACHE_LOG_DIR}/horizon.access.log combined
      #ref https://code.google.com/p/modwsgi/wiki/VirtualEnvironments
      WSGIScriptAlias / /var/www/horizon/openstack_dashboard/wsgi/django.wsgi
      WSGIDaemonProcess horizon user=www-data group=www-data processes=3 threads=10 \
        python-path=/var/www/horizon/.venv/lib/python2.7/site-packages
      WSGIProcessGroup horizon
      Alias /static /var/www/horizon/static
      SetEnv OS_CACHE True
      <Directory /var/www/horizon/openstack_dashboard/wsgi>
         Order allow,deny
         Allow from all
      </Directory>
    </VirtualHost>


Custom Hadoop Ubuntu Cloud QCOW2 Image 製作 
------------

* 先利用一台Ubuntu Instance啟動後登入(root disk 約要5G size) ::

    #安裝所需套件
    sudo -i 
    apt-get install qemu-utils  -y
    
    #可到 http://cloud-images.ubuntu.com/releases/ 上取得所需修改版本的image 
    wget http://cloud-images.ubuntu.com/releases/precise/release/ubuntu-12.04.2-server-cloudimg-amd64-disk1.img
    
    #載入nbd kernel module
    modprobe nbd

    #連接image檔到nbd裝置上
    qemu-nbd -c /dev/nbd0 `readlink -f ./ubuntu-12.04.2-server-cloudimg-amd64-disk1.img`
    
    #重調整size
    e2fsck -fp /dev/nbd0p1
    resize2fs /dev/nbd0p1

    #mount nbd裝置
    mount  /dev/nbd0p1 /mnt
    mount -o bind /dev /mnt/dev 
    mount -t proc none /mnt/proc
    mount -o bind /sys /mnt/sys
    mount -o bind /tmp /mnt/tmp
    
    #切換root到image上
    chroot /mnt /bin/bash
    
    #設定name     server
    mv /etc/resolv.conf /etc/resolv.conf.bak
    echo "nameserver 8.8.8.8" > /etc/resolv.conf
    
    #開始安裝java與hadoop套件
    add-apt-repository  ppa:webupd8team/java
    add-apt-repository ppa:hadoop-ubuntu/stable
    apt-get update
    apt-get install oracle-java6-installer oracle-java6-set-default -y
    apt-get install hadoop pig hive -y
    apt-get install python-pip zip -y
    pip install boto --upgrade
    
    #清除cache
    rm -r /var/cache/oracle-jdk6-installer
    rm -r /var/cache/apt/archives/*.deb
    
    #還原nameserver 設定
    rm /etc/resolv.conf
    mv /etc/resolv.conf.bak /etc/resolv.conf
    #返回原本root
    exit

    #umount image
    umount  /mnt/*
    umount -l /mnt
    qemu-nbd -d /dev/nbd0
    
    #之後就可將image上傳使用
    



參考文件

  * http://docs.openstack.org/developer/horizon/topics/tutorial.html


Horizon (OpenStack Dashboard)
=============================

Horizon is a Django-based project aimed at providing a complete OpenStack
Dashboard along with an extensible framework for building new dashboards
from reusable components. The ``openstack_dashboard`` module is a reference
implementation of a Django site that uses the ``horizon`` app to provide
web-based interactions with the various OpenStack projects.

For release management:

 * https://launchpad.net/horizon

For blueprints and feature specifications:

 * https://blueprints.launchpad.net/horizon

For issue tracking:

 * https://bugs.launchpad.net/horizon

Dependencies
============

To get started you will need to install Node.js (http://nodejs.org/) on your
machine. Node.js is used with Horizon in order to use LESS
(http://lesscss.org/) for our CSS needs. Horizon is currently using Node.js
v0.6.12.

For Ubuntu use apt to install Node.js::

    $ sudo apt-get install nodejs

For other versions of Linux, please see here:: http://nodejs.org/#download for
how to install Node.js on your system.


Getting Started
===============

For local development, first create a virtualenv for the project.
In the ``tools`` directory there is a script to create one for you:

  $ python tools/install_venv.py

Alternatively, the ``run_tests.sh`` script will also install the environment
for you and then run the full test suite to verify everything is installed
and functioning correctly.

Now that the virtualenv is created, you need to configure your local
environment.  To do this, create a ``local_settings.py`` file in the
``openstack_dashboard/local/`` directory.  There is a
``local_settings.py.example`` file there that may be used as a template.

If all is well you should able to run the development server locally:

  $ tools/with_venv.sh manage.py runserver

or, as a shortcut::

  $ ./run_tests.sh --runserver


Settings Up OpenStack
=====================

The recommended tool for installing and configuring the core OpenStack
components is `Devstack`_. Refer to their documentation for getting
Nova, Keystone, Glance, etc. up and running.

.. _Devstack: http://devstack.org/

.. note::

    The minimum required set of OpenStack services running includes the
    following:

    * Nova (compute, api, scheduler, network, *and* volume services)
    * Glance
    * Keystone

    Optional support is provided for Swift.


Development
===========

For development, start with the getting started instructions above.
Once you have a working virtualenv and all the necessary packages, read on.

If dependencies are added to either ``horizon`` or ``openstack-dashboard``,
they should be added to ``requirements.txt``.

The ``run_tests.sh`` script invokes tests and analyses on both of these
components in its process, and it is what Jenkins uses to verify the
stability of the project. If run before an environment is set up, it will
ask if you wish to install one.

To run the unit tests::

    $ ./run_tests.sh

Building Contributor Documentation
==================================

This documentation is written by contributors, for contributors.

The source is maintained in the ``doc/source`` folder using
`reStructuredText`_ and built by `Sphinx`_

.. _reStructuredText: http://docutils.sourceforge.net/rst.html
.. _Sphinx: http://sphinx.pocoo.org/

* Building Automatically::

    $ ./run_tests.sh --docs

* Building Manually::

    $ export DJANGO_SETTINGS_MODULE=local.local_settings
    $ python doc/generate_autodoc_index.py
    $ sphinx-build -b html doc/source build/sphinx/html

Results are in the `build/sphinx/html` directory
