.. _configurations:

Configuration Manual
====================

VLCP uses a configuration file to change the behaviors of all the internal objects.
This file determines which modules to load, which strategy the modules should use,
and which endpoints the controller should connect to or from.

.. editconfig:

===========================
Edit the Configuration File
===========================

By default the configuration file is ``/etc/vlcp.conf``, but you can modify the
configuration file position with the ``-f`` option when running ``vlcp-start``.

``#`` mark and the contents after the mark are remarks. They are removed on parsing.

Each line of the configuration file (except blank lines) should be::

   <configkey> = <configvalue>
   
``configkey`` is a name stands for the configuration target. You can find the list
of available configuration keys in ::ref:`configurationlist`. There should not be any spaces
or tabs before ``configkey``.

``configvalue`` is a literal expression in Python i.e. a Python constant like *int*, *float*,
*str*, *bytes*, *unicode*, *set*, *dict*, *list*, *tuple* or any combinations of them.
It cannot use other expressions like evaluations or function calls.

The ``configvalue`` can extend to multiple lines. The extended lines should have spaces
at the begining of the lines to identify them from normal configurations. Also they must
follow the Python grammer, means you may need to append extra ``\`` at the end of a line.

It is always a good idea to begin with `Example Configurations <https://github.com/hubo1016/vlcp/tree/master/example/config>`_

============================
Configuration Rules
============================

Usually a configuration key is made up of two or three parts::
   
   <type>.<classname>.<config>
   <classname>.<config>
   
Keys made up of three parts are configurations of a class derived from a base class like
*Protocol*, *Module*. The lower-cased base class name is ``<type>``, and the lower-cased
class name is ``<classname>``. For example, configuration keys of a module
``vlcp.service.connection.zookeeperdb.ZooKeeperDB`` begin with ``module.zookeeperdb.``,
where ``module`` is lower-cased ``Module``, ``zookeeperdb`` is lower-cased ``ZooKeeperDB``.

The base classes can also be configured. The keys begin with ``<type>.default.``. This
configuration replaces default values of all sub classes. If the same ``<config>`` is also
configured on the sub class, sub class configurations take effect. The priority order is
(from higher to lower)::

   <type>.<subclass>.<config>
   <type>.default.<config>
   (default value)

Keys made up of two parts are classes which do not have base classes and can not be inherited.

A special kind of keys begin with ``proxy.`` are proxy module configurations. A *proxy module*
is a proxy that routes abstract API calls to an actual implementation. This configuration should
be set to a string which stands for a module loading path (``<package>.<classname>``). For example::
   
   proxy.kvstorage='vlcp.service.connection.zookeeperdb.ZooKeeperDB'
   proxy.updatenotifier='vlcp.service.connection.zookeeperdb.ZooKeeperDB'
   
Set the abstract proxy ``kvstorage`` and ``updatenotifier`` to route to ``ZooKeeperDB``, thus
enables ZooKeeper as the storage service provider.

Most important configurations are ::ref:`configkey_server` and connection URLs like
``module.httpserver.url``. Other configurations usually can be left with their default values.

.. _configurationlist:

============================
All Available Configurations
============================

.. note:: This is a automatically generated list. Not all configurations are meant to be configured from
          the configuration file: some are debugging / tunning parameters; some are tended for internal
          usages.

.. contents::
   :local:

.. include:: gensrc/allconfigurations.inc
.. include:: gensrc/allproxyconfigs.inc
