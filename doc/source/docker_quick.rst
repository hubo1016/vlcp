.. _dockerquick:

Docker Integration Quick Guide
==============================

This toturial shows how to integrate VLCP into docker with *vlcp-docker-plugin*. With this customized network
plugin, you can use standard ``docker network create`` and ``docker run`` command to create networks and
containers with SDN functions. It offers more functions (L3 networking, VLAN support) and better stabilities
than the default *overlay* network plugin.

This toturial assumes the operating system to be CentOS 7, but there should be little differences for other
Linux distribution e.g. Ubuntu, Debian, Fedora. This toturial assumes you are using root account, if you run
into priviledge problems with a non-root account, you may need ``sudo``.

Most of the setup steps should be done on every server in the cluster, except ::ref:`docker_centraldatabase`,
and all the API calls with *curl* which create global objects (physical networks, physical ports).

This plugin fully supports using `Docker Swarm <https://github.com/docker/swarm/>`_ to manage the docker cluster.
with Docker Swarm, you can create containers on your cluster as if you are using a single server.

*vlcp-docker-plugin* is a separated project in `vlcp-docker-plugin GitHub Home Page <https://github.com/hubo1016/vlcp-docker-plugin>`_.

.. _docker_prepare:

-----------------------
Prepare the Environment
-----------------------

Install OpenvSwitch on each server: this is the same step as ::ref:`installovs`.

Install vlcp-docker-plugin: this is almost the same as ::ref:`preparepython`, but you must also
install vlcp-docker-plugin::
   
   pip install vlcp-docker-plugin
   
.. _docker_centraldatabase:

----------------------
Setup Central Database
----------------------

Because docker daemon and docker swarm also need a central database, and they both support ZooKeeper, so
it is always recommended to setup a ZooKeeper cluster as mentioned in ::ref:`installzookeeper`, so VLCP
can share the same cluster with docker - though not necessary.

.. _installdocker:

---------------------
Install Docker Engine
---------------------

If Docker Engine is not already installed, refer to the official document
(`Linux <https://docs.docker.com/engine/installation/linux/>`_ /
`CentOS 7 <https://docs.docker.com/engine/installation/linux/centos/>`_)
to install Docker Engine. You should specify these extra configurations, either from ``/etc/docker/daemon.json``,
or ``/etc/docker/docker.conf``, to enable multi-host networks with an external KV-storage as mentioned in
`this document <https://docs.docker.com/engine/userguide/networking/#/an-overlay-network-with-an-external-key-value-store>`_:

   ==================   =========================================  ==================================================
        Name                      Description                                 Example Value
   ==================   =========================================  ==================================================
   cluster-store        ``zk://`` started ZooKeeper cluster URL    ``zk://server1:port1,server2:port2,.../``
   ------------------   -----------------------------------------  --------------------------------------------------
   hosts                Add an extra TCP socket endpoint           ``["0.0.0.0:2375","unix:///var/run/docker.sock"]``
   ------------------   -----------------------------------------  --------------------------------------------------
   cluster-advertise    Advertise the TCP socket endpoint          ``10.0.1.2:2375``
   ==================   =========================================  ==================================================

.. caution:: It is very dangerous to expose a docker API endpoint to untrust network without protection.
             Configure *iptables* or enable tls to secure your service.

.. _createvlcpservice:

-------------------
Create VLCP Service
-------------------

Create a configuration file with the example configuration in `Git Hub <https://github.com/hubo1016/vlcp/blob/master/example/config/docker.conf>`_::
   
   curl https://raw.githubusercontent.com/hubo1016/vlcp/master/example/config/docker.conf\
         > /etc/vlcp.conf

Modify the ``module.zookeeperdb.url`` line with your ZooKeeper cluster addresses.

We will create a system service this time. First create a starting script::

   tee /usr/sbin/vlcp <<-'EOF'
   #!/bin/bash
   mkdir -p /run/docker/plugins
   rm -f /run/docker/plugins/vlcp.sock
   exec /usr/bin/vlcp-start
   EOF
   
   chmod +x /usr/sbin/vlcp

.. note:: If you are not using the default Python environment, replace ``/usr/bin/vlcp-start`` to the
          full path of the environment, and load virtualenv environment if necessary.

This script creates necessary directory structures, and clean up the socket file if it is not removed correctly,
before starting the VLCP service.

Then create a system service with *systemd* (add ``sudo`` if necessary)::

   tee /usr/lib/systemd/system/vlcp.service <<-'EOF'
   [Unit]
   Description=VLCP
   After=network.target
   Before=docker.service
   [Service]
   ExecStart=/usr/sbin/vlcp
   [Install]
   WantedBy=multi-user.target
   EOF
   
   systemctl daemon-reload
   systemctl enable vlcp
   systemctl start vlcp

The final statement starts the controller and the docker plugin. If Docker Engine is not started, you can start
it now.

.. _dockerconfigurephysicalnetwork:

--------------------------
Configure Physical Network
--------------------------

These are the same steps as ::ref:`configureopenvswitch`, ::ref:`createphysicalnetwork` and ::ref:`createphysicalport`,
but replace the OpenvSwitch bridge name with ``dockerbr0``.

.. note:: When creating physical ports, it is recommended to change the default OpenvSwitch VXLAN port with
          an extra ovs-ctl command-line option ``option:dst_port=4999``, because *overlay* network driver
          in Docker Engine also uses VXLAN port UDP 4789 for its own networking. If you use *overlay* network
          and VLCP network in the same time, the network drivers conflict with each other and make either or
          both stop working.

You may also create VLAN networks as mentioned in ::ref:`createvlanphysicalnetworks`.

.. _dockercreatenetwork:

------------------------
Create Network in Docker
------------------------

With docker plugin, creating a VLCP network in docker is the same with other network drivers::
   
   docker network create -d vlcp -o physicalnetwork=vxlan -o mtu=1450 --ipam-driver vlcp \
         --subnet 192.168.1.0/24 --gateway 192.168.1.1 test_network_a

.. note:: You may also use the corresponding docker API
          `/networks/create <https://docs.docker.com/engine/reference/api/docker_remote_api_v1.24/#/create-a-network>`_

The ``-o`` options pass options to VLCP network, as if they are passed to ``viperflow/createlogicalnetwork``.
The ``physicalnetwork`` option is necessary; others are optional. Also the `` quoted extension is supported
(Make sure you surround them with ``''`` to prevent them been executed by Shell).
Common options are:

   physicalnetwork
      Should be the ID of the physical network created in ::ref:`createphysicalnetwork`
      
   vni/vlanid
      Specify a VNI / VLAN tag instead of let VLCP choose one for you
   
   mtu
      Set MTU for this network. Usually you should set the network MTU to 1450 for VXLAN networks
      to leave space for overlay headers.
   
   subnet:...
      Options prefixed with ``subnet:`` are passed when creating the subnet, as if they are passed
      to ``viperflow/createsubnet``. Common options are:
      
      subnet:disablegateway
         Set this option to "true" or ``'`True`'`` make VLCP removes the default gateway in the container.
         This let docker creates an extra vNIC for the container, and connect the container to the
         ``docker_gwbridge`` network. If you want to use functions from the bridge network e.g.
         source NAT, port map (PAT) from/to physical server network. But you will not be able to use
         **Virtual Routers** to connect these subnets, unless you also specify ``subnet:host_routes``.
      
      subnet:host_routes
         This option creates static routes for the subnet, and they are automatically set in the
         container. This is useful for many tasks like creating site-to-site VPNs or customized
         gateways/firewalls. You may also use this option to create routes to the gateway to override
         ``subnet:disablegateway``, making it possible to use **Virtual Router** together with
         ``docker_gwbridge``
      
      subnet:allocated_start
         This option customized the allowed IP range for the containers. This should be the first
         IP address allowed to be used by the containers in this network. By default every IP address
         (except the gateway address) can be assigned to the container; with these two options, the
         IP addresses for the container are limited to this range, making it possible for a network
         to share the same address space with existed devices.
      
      subnet:allocated_end
         This is the end of the customized IP range. This should be the last IP address allowed
         to be used by the containers in this network.

You may also specify customized options. Unrecognized options are also written to the **Logical Network**
or **Subnet** configurations, they may act as metadata or serve integration purposes.

.. note:: *vlcp-docker-plugin* is both a network driver and an IPAM driver, means it can manage
          IP addresses itself. It is recommended to use ``--ipam-driver vlcp`` option to enable
          VLCP as the IPAM driver instead of using the default IPAM driver, but please be aware
          that this IPAM driver can only be used with VLCP network driver; it cannot be used
          with other network drivers. 
          
          The default IPAM driver of Docker Engine does not allow containers in different networks
          use the same IP address. In fact, different networks with a same CIDR shares the same
          address space. This may lead to difficulties on some task: creating copies of containers
          with the exactly same IP addresses for example. In contrast, VLCP IPAM driver
          always uses a separated address space for every logical network, so it is possible
          to create containers with exactly the same IP address in different networks. This
          ensures full network virtualization especially for systems which are shared by multiple
          users. Since different logical networks are explictly isolated with each other in L2,
          These duplicated IP addresses will not cause any trouble for you.

Global networks are shared among all the server nodes in a cluster. When you create the network
in any of the servers, all the other servers should be able to see and use the network.

.. _dockercreatecontainers:
          
-------------------------------------------
Create Containers in VLCP network in Docker
-------------------------------------------

It is straight forward to create a container in VLCP network::
   
   docker run -it -d --network test_network_a --name test_vlcp_1 centos
   
This uses the official CentOS image to create a new container.

.. note:: Or you can use the corresponding docker API
          `/containers/create <https://docs.docker.com/engine/reference/api/docker_remote_api_v1.24/#/create-a-container>`_
          and
          `/containers/(id or name)/start <https://docs.docker.com/engine/reference/api/docker_remote_api_v1.24/#/start-a-container>`_

The only important part is to specify the network name or ID with ``--network``. You may also
use ``--ip`` to specify an IP address, use ``--mac-address`` to specify MAC address just as
networks created by other drivers.

You can create containers on any server in the cluster with the same network. They can access
each other as soon as you create them. Try this on another server::

   docker run -it -d --network test_network_a centos ping test_vlcp_1

.. _dockerremoveobjects:

------------------------------
Remove Networks and Containers
------------------------------

*vlcp-docker-plugin* automatically remove the related objects when you use ``docker stop``, ``docker rm`` and
``docker network rm`` to remove the created objects, basicly you do not need to worry about the underlay constructure.

.. _dockerrestorepanic:

----------------------------------
Restore from a Docker Engine Panic
----------------------------------

Docker engine crashes (panic), kernel panics or power losts of physical servers create inconsistencies in docker
engine, and may lead to issues which make the containers fail to start. It is usually much easier to restore
a VLCP network created with ``--ipam-driver=vlcp`` enabled. Usually the following script fix all the problems::

   python -m vlcp_docker.cleanup -H :2375 -f /etc/vlcp.conf
   
.. note:: This script do the following jobs:
          
          1. Remove the vNICs that are already not in a container.
          2. Remove unnecessary or failed ports in OpenvSwitch.
          3. Check and remove gabage configurations in VLCP comparing with information from docker
          
          All the information will be double-checked to prevent race conditions, so it is safe to
          use this script in any situation.
          
          Notice that some problems which make the containers fail to start are not network-related
          problems, VLCP can do nothing for them. It only ensures the network part does not block you.
