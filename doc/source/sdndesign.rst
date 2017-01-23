.. _sdndesign:

SDN Design and Implementations
===============================

VLCP SDN framework allows the modules to operate Flows on a high level. With the support of lower-level
modules, flow generations are easy to understand and easy to implement.

.. _refreshonconnect:

============================
State Management of OpenFlow
============================

VLCP controller always flush all the flows in the switch when it is connected to the controller. This
makes sure the flows in the switches are consist with the view of the controller.

While the connection is alive, controller tries to add/remove/update minimal flows when necessary. This
makes the network stable on state changes.

Modules use notifications from the transaction layer to update flows. The steps are:

1. Query data from the central database

2. Update flows base on the latest data

3. Wait for update notifications from :ref:`module_objectdb`

4. If it is necessary to restart the query, goto 1; else goto 2

With the ACID guarantees from `objectdb` module, it is easy to update flows in a safe way.

.. _plugabletables:

===============
Plugable Tables
===============

In OpenFlow, tables are identified by table ID, which is a number. Processing on a packet is done from
the lower IDs to the higher IDs. This makes it difficult to extend an existed model: we need to insert
or remove tables between existing tables.

VLCP uses an extensible way to allocate table IDs for each module. It uses an unique name to identify
a table. Each table should declare none or more tables which must have smaller IDs than this table, they
are called *ancestors* of this table. This makes sure a flow can use *GOTO* instruction from these tables
to the defined table.

A table is always in one *path*. A *path* is a chain of tables which are processed one by one. VLCP inserts
a default flow for each table in a *path* to GOTO the next table in the same path, so you can insert extra tables
in a path without disturbing the original processing order. Each *path* also has an unique name, and the
name of the default *path* is an empty string `""`. Flows in tables can use *GOTO* instruction to jump
to another *path* for extra processing. Modules may also replace the default flow in a table to drop unmatched
packets or upload the packet to controller with *OPF_PACKET_IN* message.

On module loading, each module starts to acquire tables from :ref:`module_openflowmanager` with `acquiretable` API.
`openflowmanager` module queries each module with a `gettablerequest` API. The API should return a tuple::

   (table_requests, vhost_bind)
   
*vhost_bind* is a list of vhosts this module is binding to. It defaults to `[""]`, which binds only the
default vHost.

*table_requests* is the following structure::
   
   ((name1, (ancestor1_1, ancestor1_2, ...), pathname1),
    (name2, (ancestor2_1, ancestor2_2, ...), pathname2),
    ...)
    
Each line acquires a table. The first element *name* is the unique name of this table; if multiple modules acquire
the same name, it is considered to be the same table. *ancestors* are tuples of table names, they may not
be defined in this *table_requests* structure. *pathname* is the path name of this table.

For example, the module :ref:`module_ioprocessing` defines two tables::

   (("ingress", (), ''),
    ("egress", ("ingress",),''))
    
An *ingress* table and an *egress* table, all in the default path. The *egress* table must have larger ID than the
*ingress* table. If `ioprocessing` is the only SDN module loaded, there will be only two tables used in the switch.

In module :ref:`module_l2switch`, more tables are defined::

   (("l2input", ('ingress',), ''),
    ("l2output", ('l2input',), ''),
    ('egress', ('l2output', 'l2learning'), ''),
    ("l2learning", ('l2output',), 'l2learning'))

This creates *l2input*, *l2output* and *l2learning* tables. They must be in
*ingress* -> *l2input* -> *l2output* -> *l2learning* -> *egress* order. The *l2learning* table is not in the default
path, so a packet does not go through *l2output* to *l2learning* by default.

.. _sdnstrategies:

==========
Strategies
==========

Some modules can have different strategies. Usually there are three types of strategies:

Prepush
   The controller pre-creates all flows which endpoints may need to use. This has the best stabilities and performance
   for reasonable sized logical networks. When load on the central database is high, there may be a delay of a few
   seconds before the flows are created.
   
Learning
   The controller uses information from the incoming packets to create flows for outgoing packets. For example,
   input port of a packet with specified MAC address is memorised and saved to a flow. When an outgoing packet
   with the specified MAC address as the destination MAC is forwarded, the flow directs the packet to the
   original input port. If the needed flow is not created by the incoming packets, switch uses broadcast
   instead. This is the triditional way for switches to process packets. Extra broadcasting packets may be sent
   in this mode. There are two types of learning techniques:
   
   nx_learn
      This is an OpenFlow extension of OpenvSwitch. This action allows the learning procedure executed directly
      on OpenvSwitch, thus has better performance. This is recommended for very large scale of logical networks.
      
   controller learning
      This is a replacement for `nx_learn`. If you are not using OpenvSwitch (e.g. using physical switches), this
      uses OFP_PACKET_IN to upload the packet to controller for the learning procedure, which may increase the
      load of controller.
   
First-Upload
   The switch sends a packet which does not match any exising flows to controller via OFP_PACKET_IN message.
   The controller looks up the information for this packet and generate a flow for it. Further packets with the
   same properties are processed by the created flow. This introduces a quite large delay for the first packet,
   but eliminates the broadcasting packets. Usually this is not recommended.

These strategies can be configured from the module configurations, see :ref:`configurations` for details.

.. _tabledesign:

=================
Flow Table Design
=================

Current SDN modules (with L3 support) and tables they used can be expressed with the figure :ref:`figure_sdntables`:

.. _figure_sdntables:

.. figure:: _static/images/sdntables.png
   :alt: OpenFlow Tables
   
   OpenFlow Tables

Description for each table:

ingress
   This table do inital processes on the packets, initializing registers
   
l2input
   This table drops packets which should not be forwarded (e.g. STP packets, packets with broadcast source MACs).
   If learning is enabled, this table uses `nx_learn` action or OFP_PACKET_IN to creating learning flows which
   matches the source MAC with the input port.
   
vxlaninput
   If learning is enabled, this table uses `nx_learn` action to create learning flows which matches the source MAC
   with the tunnel source IP address.
   
arp
   ARP responders. Endpoints send broadcasting ARP packets to look up the MAC address for a specified IP address.
   This table directly responds these broadcasting ARP packets with the correct MAC address to eliminate these
   ARP packets.
   
l3input
   Embedded DHCP service uses this table to upload DHCP requests to the controller. Virtual routers uses this table
   to redirect packets sent to the router gateway to *l3router* table.
   
l3router
   Routing tables for each virtual router. When there is a next-hop IP address, source MAC of the packet is changed
   to the router MAC, destination MAC of the packet is changed to the next-hop MAC address; when the next-hop is
   on a connected network, goto *l3output*
   
l3output
   Lookup destination MAC address for L3 outgoing packets.
   
l2output
   Lookup the output port for this packet
   
l2learning
   If nx_learn is used, this table contains the learned flows, and is used by *l2output*
   
vxlanoutput
   Lookup the tunnel destination IP address for packets in an overlay network (VXLAN)
   
vxlanlearning
   If nx_learn is used, this table contains the learned flows, and is used by *vxlanoutput*
   
egress
   Output of packets
