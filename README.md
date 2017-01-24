# VLCP

[![Build Status](https://travis-ci.org/hubo1016/vlcp.svg?branch=master)](https://travis-ci.org/hubo1016/vlcp)
[![PyPI](https://img.shields.io/pypi/v/vlcp.svg)](https://pypi.python.org/pypi/vlcp)

VLCP is a modern SDN controller able to be integrated with OpenStack, Docker and other virtualization environments.
It is designed to be highly scalable, highly available and have very low overhead for virtual networking. 
Currently it is ready for production, and has been verified, tested and in use in clusters with about 10 physical
servers. Tests show that the controller stays stable for more than a week under high pressure as: 1000 endpoints per
server; 16+ Gbps traffic; 200 endpoint changes (creations and deletions) per minute per server.

## Why VLCP

## Functions

VLCP provides the ability to create both L2 and L3 SDN networks. All the elements in the SDN network like physical networks (infrastructures), logical networks and endpoints can be modified at any time and take an immediate effect.
Logical networks are fully isolated with each other, including the abilities to use the same MAC addresses or IP
addresses. It is very easy to create multi-tenant networks with VLCP controllers. VLCP supports both VLAN and VXLAN
for isolation; it is even possible to use them for different logical networks at the same time.

VLCP provides easy-to-use web APIs for configurations. The APIs can be used anywhere with a connection to the central
database, and multiple instances can be deployed to provide load balancing or high-availabilities.

For VXLAN, VLCP supports software implementation on OpenvSwitch, and hardware implementation with hardware_vtep
interface on physical switches (the same interface used by NSX). Software VXLAN implementation provides about
6+Gbps for each server. With physical switches supporting hardware_vtep, it is usually 20+Gbps.

### Stabilities

VLCP has a modern software architecture. It is designed to be stable under the worst situations. Usually the
controller is deployed on each server, working independently. Server failures only affects traffic to and from
this server. As long as the servers containing the source endpoint and the destination endpoint stay alive, network
traffic between these two endpoints is not affected.

VLCP uses a ZooKeeper cluster for configuration management. ZooKeeper provides consistency for all the nodes easily.
All nodes are equal to each other when reading from and writing to the central storage. They use a transaction layer
to provide ACID on multiple keys, so any change to the central storage either success or fail at once. Nodes use the
Watch mechanism of ZooKeeper to subscribe and update informations related to the local endpoints. There is not any
middle-states, any critical failures like power failures, system core dumps, network disconnections are recoverable.
The hardest recover operations of VLCP controllers are no more than restarting the controller. Usually it recovers
as soon as the network/system problems are solved.

There are multiple guarantees for the SDN network connectivities:

1. Partial failures (less than half of the servers) on the ZooKeeper cluster do not affect any operations

2. A full failure on the ZooKeeper cluster makes it impossible to create/delete/modify endpoints, but the
   existed endpoints are not affected.

3. Controller crashes on one server makes it stop responding to network structure changes (endpoint
   creation/deletion etc.), but the existed endpoints are not affected.

4. OpenvSwitch crashes, server crashes disconnect the endpoints on this server with other endpoints, but
   connectivities between endpoints on other servers are not affected.
   
5. Failures are always recoverable. No components would stay in a middle-state.
   
With these guarantees, any disasters can be keep in the smallest scope to reduce the impact to the
production environment.

## Highly Extensible

VLCP is designed to be both a production-ready SDN controller and an extensible SDN framework. Most functions
in the SDN networks are split into smaller modules, each provides an independent function. Every module can
be loaded, unloaded or reloaded even without a restarting. You can rearrange the modules to add or remove functions.
It is also possible to develop new functions with separated modules, and integrate them with the SDN controller.

# Learn More

The full document is on http://vlcp.readthedocs.io/en/latest/
