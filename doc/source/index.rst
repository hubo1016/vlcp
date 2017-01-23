.. VLCP documentation master file, created by
   sphinx-quickstart on Tue Dec 29 19:28:31 2015.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

.. _index:

   
VLCP - New generation SDN framework
===================================

VLCP is a modern SDN controller able to be integrated with OpenStack, Docker and other virtualization environments.
It is designed to be highly scalable, highly available and have very low overhead for virtual networking. 
Currently it is ready for production, and has been verified, tested and in use in clusters with about 10 physical
servers. Tests show that the controller stays stable for more than a week under high pressure as: 1000 endpoints per
server; 16+ Gbps traffic; 200 endpoint changes (creations and deletions) per minute per server.

.. toctree::
   :maxdepth: 2
   :numbered:
   
   introduce
   quickguide
   usermanual
   development
   reference
   articles

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

