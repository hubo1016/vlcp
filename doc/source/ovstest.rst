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

This toturial assumes you are using root account, if you run into priviledge problems with a non-root account,
you may need ``sudo``.

Most of the setup steps should be done on both servers, or every server in the cluster, except
::ref:`centraldatabase`, and all the API calls with *curl* which create global objects (physical networks,
physical ports, logical networks, subnets, virtual routers).

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

.. _installvlcp:

------------
INSTALL VLCP
------------

^^^^^^^^^^^^^^^^^
Install from pypy
^^^^^^^^^^^^^^^^^
Use pip tool auto install from pypy::

    pip install vlcp

^^^^^^^^^^^^^^^^^^^
Install from source
^^^^^^^^^^^^^^^^^^^
Git clone from github::
    
    git clone https://github.com/hubo1016/vlcp.git

Install use setup.py::
    
    cd vlcp && python setup.py install

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

   curl https://raw.githubusercontent.com/hubo1016/vlcp/master/example/config/genericl3.conf\
        > /etc/vlcp.conf
   
Modify the ``module.zookeeperdb.url`` line with your ZooKeeper server addresses, or if you are using Redis,
following the comments in the configuration file.


.. note:: ``module.jsonrpcserver.url='unix://var/run/openvswitch/db.sock'`` special where the UNIX socket
         which communicate with ovs. if install ovs from source , the UNIX socket file mybe
         ``unix://usr/local/var/run/openvswitch/db.sock``.

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

.. note:: You should start the controller with root priviledge (``sudo`` if necessary).

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
          UNIX socket is used, so we do not need to configure OVSDB connections with ``ovs-vsctl set-manager``.
          
          ovs fail-mode secure means ovs disconnect with controller, ovs will not set up flows on its own
          another fail-mode standalone ovs will set up flows cause the datapath to act link an ordinary MAC-learning
          switch.

From now on, if you run into some problems, or you want to retry this toturial, you can delete the whole bridge::
   
   ovs-vsctl del-br testbr0
   
And cleanup or re-install the central database.

.. _createphysicalnetwork:

-----------------------------
Create VXLAN Physical Network
-----------------------------

There is only one step to create a physical network. The example configuration open a management API port at
``http://localhost/8081``. We will call the management API with curl::

   curl -g -d 'type=vxlan&vnirange=`[[10000,20000]]`&id=vxlan' \
              'http://localhost:8081/viperflow/createphysicalnetwork?'

You may run this command on any of your server nodes. All server nodes share the same data storage, so you create
the network configuration once and they can be used anywhere.
 
The id of newly created physical network is "vxlan", this is a convient name for further calls, but you can replace
it with any name you like. If you do not specify an id, VLCP creates a UUID for you. ``vnirange`` specify a list
of VNI ranges, notice that different from *range* in Python, these ranges include **both** begin and end.
For example, ``[10000,20000]`` is 10000-20000, which has 10001 VNIs enable. Network engineers are usually more
familar with this type of ranges.


 
.. note:: By default, the management API supports HTTP GET (with query string), HTTP POST (with standard form data),
          and HTTP POST with JSON-format POST data. Though use the HTTP GET/POST format is usually the easiest way to
          call the API in Shell command-line, when integrating with other systems JSON-format POST may be more
          convient.
          
          \`\` quoted expression is a VLCP-specified extension. Some APIs need data types other than strings for its
          parameters. When a string parameter is quoted by \`\`, VLCP recognizes it as a literal expression in Python.
          You may use numbers, string, tuples, list, dictionary, sets and any combinations of them in a quoted expression.
          
          '\[\]' have special meanings in *curl*, that is way we use ``-g`` option to turn it off.

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
   
   ovs-vsctl add-port testbr0 vxlan0 -- set interface vxlan0 type=vxlan \
             options:local_ip=10.0.1.2 options:remote_ip=flow options:key=flow
   
Replace the IP address ``10.0.1.2`` to an external IP address on this server, it should be different for each server.
VLCP will use this configuration to discover other nodes in the same cluster.

.. note:: ``options:remote_ip=flow`` means vxlan dst server ip , will set use flow dynamic
         ``options:key=flow`` means vxlan tunnel id , will set use flow dynamic.

The port name ``vxlan0`` can be replaced to other names, but you should use the same name for each server.

.. note:: VXLAN uses UDP port 4789 for overlay tunneling. You must configure your *iptables* or firewall to allow UDP
          traffic on this port. If there are other VXLAN services on this server (for example, overlay network driver
          in docker uses this port for its own networking), you may specify another port by appending
          ``option:dst_port=4999`` to the commandline. Make sure all your servers are using the same UDP port.

Then create the physical port configuration (only once, on any server node)::
   
   curl -g -d 'physicalnetwork=vxlan&name=vxlan0'\
           'http://localhost:8081/viperflow/createphysicalport'
   
The ``physicalnetwork`` parameter is the physical network ID, and the ``name`` parameter is the port name in above
command.

.. _createlogicalnetworksandsubnets:

----------------------------------
Create Logical Network and Subnets
----------------------------------

In this tutorial, we will create two logical networks:
   
   * **Network A**: CIDR 192.168.1.0/24, network ID: network_a, gateway: 192.168.1.1
   * **Network B**: CIDR 192.168.2.0/24, network ID: network_b, gateway: 192.168.2.1

The steps are simple and direct. In VLCP, Ethernet related configurations are provided when createing a **Logical Network**,
and IP related configurations are provided when creating a **Subnet**. First create two logical networks::

   curl -g -d 'physicalnetwork=vxlan&id=network_a&mtu=`1450`'\
         'http://localhost:8081/viperflow/createlogicalnetwork'
   curl -g -d 'physicalnetwork=vxlan&id=network_b&mtu=`1450`'\
         'http://localhost:8081/viperflow/createlogicalnetwork'

.. note:: VXLAN introduces extra overlay packet header into the packet, so we leave 50 bytes for the header
          and set MTU=1450. If your underlay network supports larger MTU, you can set a larger MTU instead.
          The embedded DHCP service uses this configuration to generate a DHCP Option to set MTU on the
          logical port (vNIC in a virtual machine). *vlcp-docker-plugin* also uses this to generate MTU
          configurations for docker.
          
          You may use an extra parameter ``vni=10001`` to explictly specify the VNI used by this logical network.
          If ommited, VLCP automatically assign a free VNI from the physical network VNI ranges. The creation fails
          if all the VNIs in VNI ranges are used, or the specified VNI is used.

Then, create a *Subnet* for each logical network::

   curl -g -d 'logicalnetwork=network_a&cidr=192.168.1.0/24&gateway=192.168.1.1&id=subnet_a'\
         'http://localhost:8081/viperflow/createsubnet'
   curl -g -d 'logicalnetwork=network_b&cidr=192.168.2.0/24&gateway=192.168.2.1&id=subnet_b'\
         'http://localhost:8081/viperflow/createsubnet'

.. note:: There are also batch create APIs like ``createlogicalnetworks`` and ``createsubnets``, which accepts
          a list of dictionaries to create multiple objects in one transact. A batch create operation is an
          atomic operation, if one of the object is not created successfully, all the other created objects roll
          back.

.. _createlogicalports:
          
--------------------
Create Logical Ports
--------------------

We will create one logical port for each logical network and each physical server - means 4 logical ports if you have
two physical servers.

Run following commands on each server::
   
   SERVER_ID=1
   
   # create namespace
   ip netns add vlcp_ns1
   
   # create logicalport id
   LOGPORT_ID=lgport-${SERVER_ID}-1
   
   # add internal ovs interface set iface-id logicalport id
   ovs-vsctl add-port testbr0 vlcp-port1 -- set interface vlcp-port1 \
         type=internal external_ids:iface-id=${LOGPORT_ID}
   
   # get interface mac address used to create logical port
   MAC_ADDRESS=`ip link show dev vlcp-port1 | grep -oP 'link/ether \S+'\
          | awk '{print $2}'`
   curl -g -d "id=${LOGPORT_ID}&logicalnetwork=network_a&subnet=subnet_a&mac_address=${MAC_ADDRESS}"\
          "http://localhost:8081/viperflow/createlogicalport"
   
   # move interface link to namespace and up it
   ip link set dev vlcp-port1 netns vlcp_ns1
   ip netns exec vlcp_ns1 ip link set dev vlcp-port1 up
   
   # start dhcp to get ip address
   ip netns exec vlcp_ns1 dhclient -pf /var/run/dhclient-vlcp-port1.pid\
          -lf /var/lib/dhclient/dhclient-vlcp-port1.leases vlcp-port1

   # create another namespace
   ip netns add vlcp_ns2

   # create another logical port id
   LOGPORT_ID=lgport-${SERVER_ID}-2

   # add internal ovs interface set iface-id logicalport id
   ovs-vsctl add-port testbr0 vlcp-port2 -- set interface vlcp-port2 \
         type=internal external_ids:iface-id=${LOGPORT_ID}
   
   # get interface mac address used to create logical port
   MAC_ADDRESS=`ip link show dev vlcp-port2 | grep -oP 'link/ether \S+'\
         | awk '{print $2}'`
   curl -g -d "id=${LOGPORT_ID}&logicalnetwork=network_b&subnet=subnet_b&mac_address=${MAC_ADDRESS}" \
         "http://localhost:8081/viperflow/createlogicalport"
   
   # move interface link to namespace and up it
   ip link set dev vlcp-port2 netns vlcp_ns2
   ip netns exec vlcp_ns2 ip link set dev vlcp-port2 up
   
   # start dhcp to get ip address
   ip netns exec vlcp_ns2 dhclient -pf /var/run/dhclient-vlcp-port2.pid \
         -lf /var/lib/dhclient/dhclient-vlcp-port2.leases vlcp-port2
   
Change ``SERVER_ID`` to a different number for each of your server to prevent the logical port ID conflicts with
each other.

A quick description:

For each port
   
   1. Create a namespace to simulate a logical endpoint with separated devices, IP addresses and routing.
   2. Create an ovs internal port to simutate a vNIC. "external_ids:iface-id" is set to the logical port id.
   3. Use the logical port ID, logical network ID, subnet ID and the MAC address to create a new logical port configuration.
   4. Move the internal port to the created namespace.
   5. Start DHCP client in the namespace to acquire IP address configurations.

.. note:: When creating logical ports, you can specify an extra parameter like ``ip_address=192.168.1.2`` to
          explictly assign an IP address for the logical port; if omitted, a free IP address is automatically
          choosen from the subnet CIDR. See API references for details.

          *dhclient* is used to use DHCP to retrieve IP address and MTU configurations from embedded DHCP server.
          
          Use::
          
            ip netns exec vlcp_ns1 dhclient -x -pf /var/run/dhclient-vlcp-port1.pid \
                  -lf /var/lib/dhclient/dhclient-vlcp-port1.leases vlcp-port1
          
          to stop it.
          
          You may also configure the IP addresses and MTU yourself, instead of acquiring from DHCP.
          
          It is not necessary to call ``createlogicalport`` API on the same server where the ovs port is created.
          The order is also not matter (if you use a fixed MAC address). If you delete the ovs port and re-create
          it on another server, all configurations are still in effect, so you can easily migrate a virtual machine
          or docker container easily without network loss.
          
          You may also choose to omit the ``id`` parameter to let VLCP generate an UUID for you. Then you can
          set the UUID to ``external_ids:iface-id`` of the ovs port.

Now you should see the logical ports in the same logical networks can ping each other, while logical ports from
different logical networks cannot ping each other. Try it yourself::
   
   ip netns exec vlcp_ns1 ping 192.168.1.3

.. _createvirtualrouter:

---------------------
Create Virtual Router
---------------------

As you can see, logical ports in different logical networks cannot access each other with L2 packets. But you can
connect different logical networks with a **Virtual Router**, to provide L3 connectivity between logical networks.
This keeps the broadcast range of logical networks in a reasonable scale.

Let's create a virtual router and put subnet_a, subnet_b inside it::

   curl -g 'http://localhost:8081/vrouterapi/createvirtualrouter?id=subnetrouter'
   curl -g -d 'interfaces=`[{"router":"subnetrouter","subnet":"subnet_a"},\
                             {"router":"subnetrouter","subnet":"subnet_b"}]`'\
           'http://localhost:8081/vrouterapi/addrouterinterfaces'
   
Now the logical ports should be enabled to ping each other no matter which logical network they are in:

   ip netns exec vlcp_ns1 ping 192.168.2.2

.. _createvlanphysicalnetworks:

----------------------------------------
(Optional) Create VLAN Physical Networks
----------------------------------------

If your server are connected to physical switches, and the ports your server connected to are configured to
"trunk mode", and there are VLANs correctly configured and permitted in the physical switches, you may
create a VLAN physical network to connect your vNICs through VLAN network. Usually it is an easy way to
connect your vNICs to traditional networks.

It is not that different to create a VLAN physical network from creating a VXLAN physical network. We will
assume your VLAN network is connected by a physical NIC or bonding device named ``bond0``::

   curl -g -d 'type=vlan&vlanrange=`[[1000,2000]]`&id=vlan'\
          'http://localhost:8081/viperflow/createphysicalnetwork'
   curl -g -d 'physicalnetwork=vlan&name=bond0'\
          'http://localhost:8081/viperflow/createphysicalport'

And on each server::

   ovs-vsctl add-port testbr0 bond0

Creating logical networks and other parts of the network is same.

.. note:: If your VLAN network has external gateways, you may want to specify ``is_external=`True``` when creating
          subnets. When this subnet is connected to a virtual router, virtual router uses the external gateway
          as the default gateway. Static routes should be configured on the external gateway for other logical
          networks connected to the virtual router. Or you may use NAT instead, though current version does not
          support NAT yet, it is not too difficult to implement a simple source NAT solution with *iptables*.

.. _removenetworkobjects:

----------------------
Remove Network Objects
----------------------

When removing configurations from VLCP, use a reversed order: **Logical Ports**, **Virtual Router**, **Subnet**,
**Logical Network**, **Physical Ports**, **Physical Network**::

   SERVER_ID=1
   curl -g -d 'ports=`[{"id":"'"lgport-${SERVER_ID}-1"'"},
                       {"id":"'"lgport-${SERVER_ID}-2"'"}]`'\
         'http://localhost:8081/viperflow/deletelogicalports'

   curl -g -d 'interfaces=`[{"router":"subnetrouter","subnet":"subnet_a"},
                           {"router":"subnetrouter","subnet":"subnet_b"}]`'\
         'http://localhost:8081/vrouterapi/removerouterinterfaces'
   curl -g 'http://localhost:8081/vrouterapi/deletevirtualrouter?id=subnetrouter'
      
   curl -g 'http://localhost:8081/viperflow/deletesubnet?id=subnet_a'
   curl -g 'http://localhost:8081/viperflow/deletesubnet?id=subnet_b'
   curl -g 'http://localhost:8081/viperflow/deletelogicalnetwork?id=network_a'
   curl -g 'http://localhost:8081/viperflow/deletelogicalnetwork?id=network_b'
   curl -g 'http://localhost:8081/viperflow/deletephysicalport?name=vxlan0'
   curl -g 'http://localhost:8081/viperflow/deletephysicalnetwork?id=vxlan'
   
After this you can remove the ovs bridge and namespace created on each server to restore the environment::

   ip netns exec vlcp_ns1 dhclient -x -pf /var/run/dhclient-vlcp-port1.pid\
         -lf /var/lib/dhclient/dhclient-vlcp-port1.leases vlcp-port1
   ip netns exec vlcp_ns2 dhclient -x -pf /var/run/dhclient-vlcp-port2.pid\
         -lf /var/lib/dhclient/dhclient-vlcp-port2.leases vlcp-port2
   ovs-vsctl del-br testbr0   
   ip netns del vlcp_ns1
   ip netns del vlcp_ns2
