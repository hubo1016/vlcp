.. _objectdb:

Transaction Layer: ObjectDB
===========================

ObjectDB is the transaction layer in VLCP. Usually, VLCP controllers are deployed on each server. All the
information needed by the controller is stored in an external KV-database like ZooKeeper or Redis, as the
figure `figure_centralstorage` shows:

.. _figure_centralstorage:

.. figure:: _static/images/centralstorage.png
   :alt: Central Storage
   
   Central Storage
   
Every node query the central storage to get the information they care about. They may also write back to
the central storage to store state information or get synchronized with other nodes. Every node is equal
to each other, they can read or write all the keys at the same time. The transaction layer *ObjectDB* is
implemented to synchronize these read or write operations to provide atomic update for multiple keys

.. _objectdb_datastructures:

===============
Data Structures
===============

ObjectDB stores data with *DataObjects*. Each *DataObject* is a Python object which can be serialized with
JSON (or pickle, if configured). Every *DataObject* must have an unique key, which is determind by its
primary attributes (e.g. id), like
`viperflow.logicalport.docker-7c857946c3a4ba7f4e1066d7c942d7ed3b3c245a443a8f43ed19baa23c56dd73`.

The *DataObject* is serialized to JSON and stored to the KV-database with the specified key. When a node wants
to query the data of an object, it provides the key of the object and get the JSON-deserialized object.

When an object need to reference other *DataObjects*, it stores its key to one of its attributes, or stores the
keys to a list. When the object is retrieved, the program can further retrieve the referenced objects.

But there are some problems:

1. When we update a *DataObject*, we read the object from the database, update the value and write it back.
   This may overwrite other updates between the read and the write. (Not **Isolated**)

2. When we update multiple *DataObjects* at the same time, some of them may success while others may fail.
   (Not **Atomic**)

3. When two nodes both update two *DataObjects* A and B, there is a chance that the final result of A and B
   are from different nodes (Not **Consistent**)
   
4. When we retrieve a node, and try to retrieve further references, the references may already be changed by
   other nodes (Not **Consistent**)
   
5. When we updated multiple keys, a remote node may only update part of them (Not **Consistent**)

That is why we need a transaction layer to solve these problems.

.. _objectdb_design:

============
Basic Design
============

The basic design of ObjectDB is shown in figure :ref:`figure_objectdb`:

.. _figure_objectdb:

.. figure:: _static/images/objectdb.png
   :alt: ObjectDB Basic Design
   
   ObjectDB Basic Design

ObjectDB depends on two components: the *KVStorage* module and the *Notifier* module. A *KVStorage* module
provides two basic interfaces:

.. py:method:: KVStorage.updateallwithtime(keys, updater)
   :noindex:
   
   Basic write transaction on the storage. The update process must be atomic.
   
   :param keys: a tuple of keys of DataObjects
   
   :param updater: a python function
                   
                   .. py:function:: updater(keys, values, timestamp)
                      :noindex:
                      
                      A function describing a transaction. The function may be called more than once and it
                      must return the same result with the same parameters. It cannot be a routine method.
                      
                      :param keys: a tuple of keys of DataObjects. It is the same as the keys in `updateallwithtime`.
                      
                      :param values: a tuple of DataObject values. If the object do not exist in the storage, the
                                     corresponding value is None.
                                     
                      :param timestamp: a server side timestamp from the central database with nano-seconds
                      
                      :return: `(updated_keys, updated_values)` to write to the central database.
                      
                      If the function raises any exception, the transaction is aborted.
   
   :return: the final return value of `updater`

.. py:method:: KVStorage.mget(keys)
   :noindex:
   
   Basic read transaction on the storage. The read process must be atomic.
   
   :param keys: a tuple of keys of DataObjects
   
   :return: the DataObjects corresponding to the keys. If a DataObject is not found in the central database, `None`
            is returned for this key.
            
These two methods provide the basic abilities for transaction. It is implemented with the lower-level KV-database
functions, for example, Redis uses the "WATCH/MULTI/EXEC" procedure, and ZooKeeper uses the `MultiRequest` command.

ObjectDB creates *DataObject* caches for all the watching keys. The cache is called the *data mirror*. All routines
that query data from ObjectDB get references of the *DataObjects*. The *ReferenceObject* is a proxy object which
reads the attributes from the *DataObject* but prevent writing to them. This makes sure the *DataObject* is correctly
shared between different routines.

.. _objectdb_notification:

=========================
Transact and Notification
=========================

A write transact is done with `updateallwithtime`, so it is natually a transact operate. After the transact,
a notification is sent to this node and other nodes.

Notifications contain the full list of keys that are updated. When nodes receive this notification, it always
retrieve these updated keys with a `mget`, so the view on each node is always consistent.

Notification for this node is from a shortcut to let the data mirror been updated immediately.

.. _objectdb_walk:

===========
Walk Method
===========

ObjectDB provides a walk method to retrieve related *DataObjects* at once. This method uses `walker` functions to
retrieve data:

.. py:function:: walker(key, object, walk, save)
   :noindex:
   
   A function describing a reading transact. The function may be called more than once. It should use `walk`
   and `save` interactively to retrieve the results. If the function raises an exception, the transaction
   is aborted.
   
   :param key: The key of the initial starting object.
   
   :param object: The value of the initial starting object.
   
   :param walk: A function to retrieve a DataObject:
                
                .. py:function:: walk(key)
                   :noindex:
                   
                   :param key: key of a DataObject to retrieve
                   
                   :return: the DataObject value, or None if not existed.
                   
                   :raise KeyError: if the key has not been retrieved from the central database yet.
                                    The walker function should catch this exception to stop further retrieving.
                                    ObjectDB will call `walker` again after the keys are retrieved.
   
   :param save: A function to save a retrieved key:
                
                .. py:function:: save(key)
                   :noindex:
                   
                   :param key: key of a DataObject to save. It must be either the original `key` when `walker`
                               is called, or has been retrieved with `walk`
                               
Saved keys from the walker is returned from ObjectDB, and is registered to ObjectDB as been *watching*. A key been
watching receives update notifications when it is updated by other operatings either from this node or from other
nodes. Use `unwatch` to cancel monitoring of the key.

When the walk method is called, ObjectDB first tries to execute the walkers in the current data mirror. If there
are keys that are not retrieved, ObjectDB tries to retrieve **all keys that are used by the walker** with `mget`.
After the first loop, there would be three types of *DataObjects*:

1. *DataObjects* that are newly retrieved by the last `mget`

2. *DataObjects* that are retrieved at least once, but not retrieved by the last `mget`

3. *DataObjects* that are in the data mirror and have not been retrieved since the first loop.

A walker is executed either with only *DataObjects* in (1), or only *DataObjects* in (2)(3). That means the walker
is always executed in a consistent dataset. This is described with figure :ref:`figure_walkers`:

.. _figure_walkers:

.. figure:: _static/images/walkers.png
   :alt: Isolation of Data Space for walkers
   
   Isolation of Data Space for walkers


If update notifications are received during the updating procedure, the keys are updated with the same `mget`.
If a `mget` retrieves *DataObjects* that are newer than the latest update notification, ObjectDB waits for the
update notification to update all the other keys at the same time.

