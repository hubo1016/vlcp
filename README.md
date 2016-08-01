# VLCP
A full stack framework for SDN Controller, support Openflow 1.0, Openflow 1.3, and Nicira extensions.

VLCP 1.0 is now released, it is a fully functional SDN controller framework, with an asynchonous IO framework embedded as the core,
and shipped with the necessary modules to form a powerful L2 SDN controller. You may benefit from this project either as an end user
or as a developer of your own controller.

Since there is an embedded asynchonous IO framework, the VLCP itself is a powerful coroutine-based web server. It is quite easy to
deploy a web service or a full web site with this framework. An empty page with session enabled benchmarks 700qps in CPython and
2000qps in PyPy. It is especially useful for comet (long-poll) services.

See examples/webIM for a simple Web Chat example.

# Try

To install the latest release version, use pip:
```bash
pip install vlcp
```

You will need at least two host servers (or virtual machines, for test purpose) with OpenvSwitch 2.3+ installed, connected by a
local network. VLCP uses Redis as the main database, so you should install Redis server on one of the host servers
(or another server which is accessable from any of the host servers), and configure it to be accessible from other
servers and persistence (AOF or RDB) enabled.

more configurations - TBD

# Technical details

VLCP processes all the Openflow messages, Ethernet packets, DHCP messages and other binary structures in a standard way,
it is now a separated library namedstruct (https://github.com/hubo1016/namedstruct, nstruct in PyPI). It is like a
*regular expression* in binary data. It allows you create a definition of the struct just like what is in openflow.h:

```Python
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
```

See documents of namedstruct for more information.

The coroutine core is an special-designed and easy-to-use one. It uses a pub/sub logic for synchronization. Comparing to other
sychronization logic like locks, Futures and channels, the pub/sub events are easy to understand, easy to use and easy to extend.

Coroutines in VLCP are generators. With each yield statement, the coroutine waits for some type of events, and continue to execute.
A simple example looks like this:

```Python

@withIndices('type')
class MyEvent(Event):
    pass

container = RoutineContainer(scheduler)
    
def routineA():
    for m in container.waitForSend(MyEvent(type = 1, message = 'event1')):
        yield m
    for m in container.waitWithTimeout(2):
        yield m
    for m in container.waitForSend(MyEvent(type = 2, message = 'event2')):
        yield m

def routineB():
    # Match for event of type 'MyEvent'
    my_matcher = MyEvent.createMatcher()
    while True:
        yield (my_matcher,)
        print(container.event.type, container.event.message)

def routineC():
    # The event is broadcasted to all the matchers, so there can be more than one coroutine to process it
    # You can specify index value to classify the event and match only a part of them
    my_matcher = MyEvent.createMatcher(type = 2)
    while True:
        yield (my_matcher,)
        print(container.event.type, container.event.message)
        
```
