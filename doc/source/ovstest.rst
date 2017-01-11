.. _ovstest:

Basic Test with OpenvSwitch
===========================

This tutorial creates a simple SDN test environment with two physical servers (or virtual machines instead).
If you are confused with some concepts, read :ref:`quickunderstand` first. 

You need two physcial servers with namespace support for this tutorial. This tutorial assumes you are using
CentOS 7, but any Linux version in which "ip netns" command is available can be used. If you want to try
VLAN networks, you may need to configure the physical switch port to "trunk mode". If that is not possible,
you can still use VXLAN isolation, in that case, you may use virtual machines (even in public cloud) to replace
physical servers.

Most of the setup steps should be done on both servers, or every server in the cluster, except
*Setup Central Database*.

.. _installovs:

-------------------
Install OpenvSwitch
-------------------

Download OpenvSwitch releases from `http://openvswitch.org/download/ <http://openvswitch.org/download/>`_.
You may choose a version from 2.3x, 2.5x or higher. 2.5x is recommended. 

Build and install OpenvSwitch, following the steps in
`http://www.openvswitch.org/support/dist-docs-2.5/INSTALL.RHEL.md.html <http://www.openvswitch.org/support/dist-docs-2.5/INSTALL.RHEL.md.html>`_.
Usually you may build a RPM package once and install it on each server nodes with *yum*.

.. note:: Other Linux distributions may have pre-built OpenvSwitch packages available,
          check the version if you want to use it instead.

.. _preparepython:

--------------------------
Prepare Python Environment
--------------------------

VLCP works in Python 2.6, Python 2.7, Python 3.3+ and PyPy2. For production environment, PyPy is recommended
which is about 5x faster than CPython. The most simple way to use PyPy is using the
`Portable PyPy distribution for Linux <https://github.com/squeaky-pl/portable-pypy#portable-pypy-distribution-for-linux>`_.
For test purpose, using the default CPython (2.7 in CentOS 7) is enough. You may also use a *virtualenv* environment
if you want.

Use pip to install VLCP. If pip is not ready, refer to `https://pip.pypa.io/en/stable/installing/ <https://pip.pypa.io/en/stable/installing/>`_.
If you are using old versions of pip, you may also want to upgrade pip, setuptools and wheel::
   
   pip install --upgrade pip setuptools wheel

Install VLCP::

   pip install vlcp

Some optional packages can be used by VLCP, install them as needed:

   hiredis
      If you use Redis for the central database, install hiredis will improve the performance
   
   python-daemon
      Support daemonize with "-d" option to create system service for VLCP. But it is usually more convient to use
      with *systemd* in CentOS 7.

.. _centraldatabase:

----------------------
Setup Central Database
----------------------

VLCP uses external KV-storage as a central database for configuration storage and distributed coordination.
Currently *Redis* and *ZooKeeper* is supported. *ZooKeeper* is recommended for its high availability with clustering.

Choose **one of the follwing sections** to proceed. You may install the central database on one of the physical servers
or on another server.

.. _installzookeeper:

^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
Install ZooKeeper (Recommended)
^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^

Follwing steps in `http://zookeeper.apache.org/doc/current/zookeeperStarted.html <http://zookeeper.apache.org/doc/current/zookeeperStarted.html>`_.
For this tutorial, install a standalone node is enough, but you should setup a cluster for production environment.

.. note:: You should protect the server port 2181 with *iptables* to grant access only from your servers.

.. _installredis:

^^^^^^^^^^^^^^^^^^^^^^^^
Install Redis (Optional)
^^^^^^^^^^^^^^^^^^^^^^^^

Download, compile and install redis from `https://redis.io/download <https://redis.io/download>`_. You should configure
redis server to listen to 0.0.0.0 or a IP address available for all the servers. You may also want to enable AOF.

.. caution:: It is a huge security hole to open redis port on Internet without password protection: attackers can
             grant full controll of your server by overwriting sensitive files like ~/.ssh/authorized with administration
             commands. Make sure you use private IP addresses, or configure *iptables* or your firewall correctly to
             allow connecting only from your servers.
             
             Newer versions of Redis also blocks this configuration with a "protect mode", you may need to disable
             it after you configure *iptables* correctly.

.. _createconfiguration:

-------------------------
Create VLCP Configuration
-------------------------

Download example configuration file from `GitHub <https://github.com/hubo1016/vlcp/tree/master/example/config>`_
and save it to ``/etc/vlcp.conf`` as a start. In this tutorial, we will use genericl3.conf::

   curl https://raw.githubusercontent.com/hubo1016/vlcp/master/example/config/genericl3.conf > /etc/vlcp.conf
   
Modify the ``module.zookeeperdb.url`` line with your ZooKeeper server addresses, or if you are using Redis,
following the comments in the configuration file.

.. _startvlcpservice:

------------------
Start VLCP Service
------------------

It is usually a good idea to create a *system service* for VLCP to make sure it starts on server booting,
but for convient we will start VLCP with *nohup* instead::

   nohup vlcp-start &

or::
   
   nohup python -m vlcp.start &

To stop the service, run command ``fg`` and press Ctrl+C, or use *kill* command on the process.

.. _configureopenvswitch:

---------------------
Configure OpenvSwitch
---------------------

Create a test bridge in OpenvSwitch for virtual networking. The name of the bridge usually does not matter.
In this tutorial we use ``testbr0``. For docker integration, the bridge name ``dockerbr0`` is
usually used. Run following commands on each server::

   ovs-vsctl add-br testbr0
   ovs-vsctl set-fail-mode testbr0 secure
   ovs-vsctl set-controller testbr0 tcp:127.0.0.1

This creates the test bridge and the OpenFlow connection to the VLCP controller.

.. note:: VLCP communicates with OpenvSwitch in two protocols: OpenFlow and OVSDB (a specialized JSON-RPC protocol).
          Usually the SDN controller is deployed on the same server with OpenvSwitch, in that case the default OVSDB
          UNIX socket is used, so we do not need to configure OVSDB connections with ``ovs-vsctl set-manager``

.. _createphysicalnetwork:

-----------------------------
Create VXLAN Physical Network
-----------------------------

There is only one step to create a physical network. The example configuration open a management API port at
``http://localhost/8081``. We will call the management API with curl::

   curl -g 'http://localhost:8081/viperflow/createphysicalnetwork?type=vxlan&vnirange=`[[10000,20000]]`&id=vxlan'

You may run this command on any of your server nodes. All server nodes share the same data storage, so you create
the network configuration once and they can be used anywhere.
 
The id of newly created physical network is "vxlan", this is a convient name for further calls, but you can replace
it with any name you like. If you do not specify an id, VLCP creates a UUID for you. ``vnirange`` specify a list
of VNI ranges, notice that different from *range* in Python, these ranges include **both** begin and end.
For example, ``[10000,20000]`` is 10000-20000, which has 10001 VNIs enable. Network engineers are usually more
familar with this type of ranges.


 
.. note:: By default, the management API supports HTTP GET (with query string), HTTP POST (with standard form data),
          and HTTP POST with JSON-format POST data. Though use the HTTP GET format is usually the easiest way to
          call the API in Shell command-line, when integrating with other systems JSON-format POST may be more
          convient.
          
          \`\` quoted expression is a VLCP-specified extension. Some APIs need data types other than strings for its
          parameters. When a string parameter is quoted by \`\`, VLCP recognizes it as a literal expression in Python.
          You may use numbers, string, tuples, list, dictionary, sets and any combinations of them in a quoted expression.

.. _createphysicalport:

--------------------
Create Physical Port
--------------------

Every physical network need one physical port for each server to provide external connectivity. There are two steps
to create a physical port:
   
   1. Create the port on each server and plug the port to the bridge
   2. Create the physical port configuration in VLCP
   
.. note:: These two steps can be done in any order. When you extend a cluster, you only need to do Step 1. on new
          servers since the second step is already done.
          
First create a vxlan tunnel port in each server::
   
   ovs-vsctl add-port dockerbr0 vxlan0 -- set interface vxlan0 type=vxlan options:local_ip=10.0.1.2 options:remote_ip=flow options:key=flow
   
Replace the IP address ``10.0.1.2`` to an external IP address on this server, it should be different for each server.
VLCP will use this configuration to discover other nodes in the same cluster.

The port name ``vxlan0`` can be replaced to other names, but you should use the same name for each server.

.. note:: VXLAN uses UDP port 4789 for overlay tunneling. You must configure your *iptables* or firewall to allow UDP
          traffic on this port. If there are other VXLAN services on this server (for example, overlay network driver
          in docker uses this port for its own networking), you may specify another port by appending
          ``option:dst_port=9999`` to the commandline. Make sure all your servers are using the same UDP port.

Then create the physical port configuration (only once, on any server node)::
   
   curl -g 'http://localhost:8081/viperflow/createphysicalport?physicalnetwork=vxlan&name=vxlan0'
   
The ``physicalnetwork`` parameter is the physical network ID, and the ``name`` parameter is the port name in above
command.

.. _createlogicalnetworksandsubnets:

----------------------------------
Create Logical Network and Subnets
----------------------------------

