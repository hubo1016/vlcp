.. _modulelist:

Function Module List
====================

VLCP function modules are classes that can be loaded to / unloaded from the server dynamically.
With different modules loaded, VLCP can provide very different functions. Users may decide which
functions they would like to use by configurating ``server.startup`` configuration.

.. _startup:

==============================
Server Start/Stop and Modules
==============================

When VLCP server starts, all startup modules in ``server.startup`` are loaded into
server and started. They each start their own routines to provide their services.
Some modules have dependencies on other modules, the dependencies are automatically
started first.

The ``server.startup`` configuration is a tuple of *loading paths* of modules.
A module is actually a Python class derived from :py:class:`vlcp.service.module.Module`.
The *loading path* of a module is the full path of the Python module file contains the
class, followed by a dot and the class name, like ``vlcp.service.connection.httpserver.HttpServer``.
The *module name* of a module is the lower-cased class name, like ``httpserver``.
VLCP modules with the same *module name* cannot be loaded or used together.

Server stop when all the routines stop, this is usually when:

   * All connections and server sockets are closed, and all pended jobs are done
   
   * The server receives an end signal (SIGINT, SIGTERM) and stops all the routines
   
   * All modules are unloaded

.. _moduleapis:

===========
Module API 
===========

Modules provide functions through a lot of methods, the most important one of which
is the **Module API**. Module APIs are methods exposed by the module to be called
from other modules, or directly from the end user.

Parameters for Module APIs are always provided with keyword-arguments: arguments
positions do not matter.

.. _modulecallfromwebapi:

^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Call Module API from WebAPI
^^^^^^^^^^^^^^^^^^^^^^^^^^^^

A management module :ref:`module_webapi` exposes all module APIs through a management
HTTP service. Through this management function, you can call module APIs with *curl*::
   
   curl -g -d 'param1=value1&param2=value2' \
      'http://localhost:8081/modulename/apiname'

The URL path of the request should be /*modulename*/*apiname*, where *modulename* is the
module name i.e. the lower-cased class name, and *apiname* is the API name i.e. the
lower-cased method name. 

By default, the management API supports HTTP GET (with query string), HTTP POST (with standard form data),
and HTTP POST with JSON-format POST data. Though use the HTTP GET/POST format is usually the easiest way to
call the API in Shell command-line, when integrating with other systems JSON-format POST may be more
convient.

\`\` quoted expression is a VLCP-specified extension. Some APIs need data types other than strings for its
parameters. When a string parameter is quoted by \`\`, VLCP recognizes it as a literal expression in Python.
You may use numbers, string, tuples, list, dictionary, sets and any combinations of them in a quoted expression::

   curl -g -d 'complexparam=`[1,2,{"name":("value",set(1,2,3))}]`' \
      'http://localhost:8081/modulename/apiname'

Make sure to surround the \`\` expression with \'\' to prevent it from excuting as a shell command.

Also notice that '\[\]' have special meanings in *curl*, that is why we use ``-g`` option to turn it off.

The return value of the module API is formatted with JSON-format, return with ``{"result": <data...>}``
format. If any error occurs, the HTTP request returns a 4xx or 5xx HTTP status, with a JSON body
``{"error": "<errormessage>"}``. Exception details can be found in system logs.

.. _modulecallfromdebuggingconsole:

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Call Module API from Debugging Console
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

When using debugging console module :ref:`module_console`, you can use callapi() method to call module
APIs easily in the debugging console as other modules. This method accepts keyword-arguments. for example::

    for m in callapi(container, "objectdb", "getonce",key="xx"):
        yield m

will call objectdb module api getonce. 

.. note:: debugging console module will also start telnet server on localhost:9923
          you can choose telnet it when server run in daemon mode.


.. _moduleapidiscovery:

^^^^^^^^^^^^^^^^^^^^
Module API Discovery
^^^^^^^^^^^^^^^^^^^^

Every module supports a special API ``discovery``. When discovery is called, a list of supported endpoints
and their descriptions are returned. With an extra parameter ``details=true``, it also returns information
for arguments and their default values. For example, you can call::

   curl -g 'http://localhost:8081/viperflow/discover?details=true' | python -m json.tool

To view the API details of module `viperflow`.


.. _reloadmodules:

======================================
Dynamic Load / Unload / Reload Modules
======================================

If :ref:`module_manager` module is loaded, you can use its APIs to load, unload or reload modules
on the fly. When reloading modules, the files containing VLCP module classes are reloaded to use
the latest code on disk, so it is possible to upgrade modules without stopping the service.

.. note:: This should be considered as an advanced feature. Not all modules are strictly tested
          against reloading. Use this function with caution.
          
.. _allmodulelist:

================
All Module List
================

These are modules currently shipped with VLCP and vlcp-docker-plugin.

.. note:: This is a generated list. Not all module APIs are designed to be used by end users directly.
          Lots of them are providing services for other modules.

.. contents::
   :local:

.. include:: gensrc/allmodulelist.inc

.. _proxymodulelist:

==================
Proxy Module List
==================

Proxy modules are configurable modules that proxy abstract API requests to different implementations. See
:ref:`configkey_proxy` for more information.

.. include:: gensrc/allproxymodules.inc

