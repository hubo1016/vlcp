.. _configurationdesign:

VLCP Configuration Desigin and Implementations
==============================================

As :ref:`configurations` description, VLCP use config file to change the behaviors of all the internal objects.
each key value in file will be store into ``ConfigTree`` an data structure named ``manager``. every key made up of
multi parts split by . , describe an inherit relationship, so config key has a close relationship with source class code.
class instance attribution will map with ``ConfigTree`` , so we can change value through config file. The steps are:

    1. read config file to manager ConfigTree.
    2. map class instance attribution with ConfigTree.


.. readconfigfile

==============================
Read config file to ConfigTree
==============================
When VLCP start, it will read config , parse it , and store it to ``manager``. as code below::

    server.py:: main: manager.loadfrom(configpath)
    config.py:: manager: loadfromfile(file)

every key value will be store as ``manager`` attribution.


.. note:: ConfigTree class has implements __setitems__ , so you can read config.py
           to know how to store attribution.


.. mapclassinstance

=============================
Map class instace attribution
=============================
After perpare ``ConfigTree`` , every class want to map attribution to manager ``ConfigTree``, must be inherit from
``Configurable`` an class has implements ``__getattr__`` method. as steps::

    1. try return manager[getattr(cls, 'configkey') + '.' + key] as value
    2. try return cls.__dict__['_default_' + key] as value
    3. try parent class goto 1

so when class get attribution will get manager value first.

but as step 1 we need class ``configkey`` attribution to find value. so class must have ``configkey`` attribution also.
there is three help method in ``config.py`` to create configkey attribution::

    configbase(key)
    config(key)
    defaultconfig(key)

for example an configurable class mybe link this::

    @config('server')
    class Server(Configurable):
        ......


.. note:: configkey has relationship with base class , you can read function declaration
           in source code.

