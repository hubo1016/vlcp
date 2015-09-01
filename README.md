# vlcp
A full stack framework for SDN Controller, support Openflow 1.0, Openflow 1.3, and Nicira extensions.

At first I decide to name the project as "Very Lightweight Controller in Python". After I finished most of the important parts I found it not lightweight at all. Anyway I can not think out of a better name now.

This is a project written in Python, and is compatible for Python 2.6, Python 2.7, Python 3.x and PyPy. The recommended running environment is PyPy.

The project now depends on EPoll, so it only run correctly on linux.

There is already a lot of Controller frameworks even in Python, like Ryu and POX. Still there IS something in this project that may attract you.

# Highlight

This project uses a very convient and efficient way to process Openflow messages. We all know that Openflow messages are highly dynamic raw data structures without version compatibility, which often makes the serializing and deserializing code look like hell. In this project, instead of creating many classes for the messages, the message structures are defined with a common format 'namedstruct':


	'''
	/* Description of a port */
	'''
	ofp_port = nstruct(
	    (ofp_port_no, 'port_no'),
	    (uint8[4],),
	    (mac_addr, 'hw_addr'),
	    (uint8[2],),                #  /* Align to 64 bits. */
	    (char[OFP_MAX_PORT_NAME_LEN], 'name'), # /* Null-terminated */
	
	    (ofp_port_config, 'config'),     #   /* Bitmap of OFPPC_* flags. */
	    (ofp_port_state, 'state'),      #   /* Bitmap of OFPPS_* flags. */
	
	#    /* Bitmaps of OFPPF_* that describe features.  All bits zeroed if
	#     * unsupported or unavailable. */
	    (ofp_port_features, 'curr'),       #   /* Current features. */
	    (ofp_port_features, 'advertised'), #   /* Features being advertised by the port. */
	    (ofp_port_features, 'supported'),  #   /* Features supported by the port. */
	    (ofp_port_features, 'peer'),       #   /* Features advertised by peer. */
	
	    (uint32, 'curr_speed'), #   /* Current port bitrate in kbps. */
	    (uint32, 'max_speed'),  #   /* Max port bitrate in kbps */
	    name = 'ofp_port'
	)

which is quite the same as they are in openflow.h. In fact most of the structures are modified from the C header file with some rules. The structure can parse itself from a byte stream as soon as you define it with these tuples. There is even more powerful functions that display the structures with human-readable format, like translate the enumerators to names, or display a MAC address with 'xx:xx:xx:xx:xx:xx' format, all with some kind of definitions.

In this project, the socket layer (connection) and protocol layer (protocol) are separated, newly created protocols only need to concern on parsing and formating messages. All the network transmit parts are in common.

Now the project supports both Openflow and OVSDB. It even has a HTTP protocol implementation, which is fully comptabitle with the latest HTTP/1.1 standard RFC723x. It serves about 2k qps with keep-alive enabled or 1k qps without keep-alive (tested in PyPy with ab) and is suitable for REST APIs or event web consoles.

# Architecture
Different from modern network projects written with Greenlet (or Eventlet), this project does not depend on 3rd-party libraries. The coroutines are mainly created with language-integraded generators, which made it annoying when you have to keep writing code like:

	for m in container.waitForSend(event):
	  yield m

It should not be too hard to rewrite the code with greenlet, and I'm looking forward to a greenlet-based fork.

All these generators are driven with events. The events may be different from event in other projects in some ways:
1. It is always sent into a queue and triggered asynchronously
2. It has a group of indices to mark unique properites of the event, e.g. datapath id and type of a Openflow message. A coroutine matches events with indices, and only respond to the correct events
3. The event can be marked as "cannot ignore", which must be processed before discarding. If the event is not processed, it will block the queue until some coroutines correctly process it.

The events themselves provide a complete way for notify and synchronization. They can be reorded by queues.

Queues in this program likes the tc class-based queues in linux. They have matchers to classify the events and put them into different queues, so one event only block the sub queue and still let other queues work properly. Queues usually have limits, it may reject an event when it is full. Queues also generate events like QueueCanWrite(queue is not full now), and QueueIsEmpty(queue is empty now). Event senders wait for these events to continue sending the event, So if the events are not consumed regularly, the queue will stop the producer of events and block them on sending more events.

A generator simply yield a group of event matchers to wait for the matched events. Any event will wake up the generator coroutine, and the generator checks the event and decide what to do next. It is much easier than the event handler model and can write very complicated logic, much the same as greenlet. And event excuting orders are only decided by queue setups.

**Author: Hu Bo**
