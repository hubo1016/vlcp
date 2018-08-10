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
                                     
                      :param timestamp: a server side timestamp from the central database with micro seconds
                      
                      :return: `(updated_keys, updated_values)` to write to the central database.
                      
                      If the function raises any exception, the transaction is aborted.
   
   :return: the final return value of `updater`

.. py:method:: KVStorage.mgetwithcache(keys, cache = None)
   :noindex:
   
   Basic read transaction on the storage. The read process must be atomic.
   
   :param keys: a tuple of keys of DataObjects
   
   :return: `(result, cache)` tuple. `result` is the DataObjects corresponding to the keys.
            If a DataObject is not found in the central database, `None` is returned for this key.
            `cache` is a cache object used by later calls, to cache necessary information for
            acceleration (for example, if the data is not changed, storage module can return the
            exact same instance stored in the cache object by previous calls).
            
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

A read-only transaction uses `mgetwithcache` to get values in the same DB version. A read-only transaction can only use
data from the same DB version. If some necessary is missing or out-dated, a `mgetwithcache` is used to retrieve all
the needed keys from KVStorage.

A read-write transact is done with `updateallwithtime`, so it is natually a transact operate. After the transaction,
a notification is sent to this node and other nodes.

Notifications contain the full list of keys that are updated. When nodes receive this notification, it always
retrieve these updated keys with a `mgetwithcache`, so the view on each node is always consistent.

Notification for this node is from a shortcut to let the data mirror been updated immediately.

.. _objectdb_walk:

===========
Walk Method
===========

Walker method are high-level transaction methods, they provide generic transaction ability, and are easy to use.

ObjectDB provides a `walk` method for read-only transaction. It retrieve related *DataObjects* at once.
It uses `walker` functions to retrieve data:

.. py:function:: walker(key, object, walk, save)
   :noindex:
   
   A function describing a reading transaction. The function may be called more than once when executed in `ObjectDB.walk`.
   It should use `walk` and `save` interactively to retrieve the results. If the function raises an exception, the transaction
   is aborted.
   
   :param key: The key of the initial starting object.
   
   :param object: The value of the initial starting object.
   
   :param walk: A function to retrieve a DataObject:
                
                .. py:function:: walk(key)
                   :noindex:
                   
                   :param key: key of a DataObject to retrieve
                   
                   :return: the DataObject value, or None if not existed.
                   
                   :raise `vlcp.utils.exception.WalkKeyNotRetrieved`: raised if the key has not been retrieved from the central database yet.
                                                                      The walker function should catch this exception and stop further retrieving
                                                                      depends on the return value. ObjectDB will call `walker` again after the keys are retrieved.
                                                                      
                                                                      `WalkKeyNotRetrieved` exception is a subclass of `KeyError`
   
   :param save: A function to save a retrieved key:
                
                .. py:function:: save(key)
                   :noindex:
                   
                   :param key: key of a DataObject to save. It must be either the original `key` when `walker`
                               is called, or has been retrieved with `walk`
                               
Saved keys from the walker is returned from ObjectDB, and is registered to ObjectDB as been *watching*. A key been
watching receives update notifications when it is updated by other operatings either from this node or from other
nodes. Use `unwatch` to cancel monitoring of the key.

When the walk method is called, ObjectDB first tries to execute the walkers in the current data mirror. If there
are keys that are not retrieved, ObjectDB tries to retrieve **all keys that are used by the walker** with `mgetwithcache`.

Each `mgetwithcache` call creates a different version of data mirror.
Data mirror before current execution is version -1. For each retrieved key, the valid version range is calculated.
For example, if key *A* is in data mirror before execution (version -1), and is retrieved at version 4, but the
value is not changed, then the valid version range is [-1, 4], closed. If key *B* is retrieved in version 1, and version 4,
but the value in version 4 has been changed, then the valid version range is [4, 4].

When a walker is executing, only keys that has at least one compatible data mirror version can be retrieved.
That means the walker is always executed in a consistent dataset. This is described with figure :ref:`figure_walkers`:

.. _figure_walkers:

.. figure:: _static/images/walkers.png
   :alt: Isolation of Data Space for walkers
   
   Isolation of Data Space for walkers

When the keys needed do not have a compatible data mirror version, all the keys will be retrieved with `mgetwithcache`
in the next version, so they will have a compatible version on next execution. If some values are changed, the keys retrieved
by the walker may differ from the previous execution. This will continue until the walker
can successfully finish executing in a complete and compatible dataset.

If update notifications are received during the updating procedure, the keys are updated with the same `mgetwithcache`.
If a `mgetwithcache` retrieves *DataObjects* that are newer than the latest update notification, ObjectDB waits for the
update notification to update all the other keys at the same time. When the values are updated, all the related walkers
are restarted to use the latest value.

.. _objectdb_writewalk

==================
Write Walk Methods
==================

Write walk methods are high-level read-write transaction methods. Similar to walk, a `walker` function is needed. The parameters
are slightly different, and only one `walker` function is needed:

.. py:function:: walker(walk, write)
   :noindex:
   
   A function describing a read-write transaction. The function may be called more than once when executed in `ObjectDB.writewalk`.
   It should use `walk` and `write` interactively to modify values. If the function raises an exception, the transaction
   is aborted.
   
   :param walk: A function to retrieve a DataObject:
                
                .. py:function:: walk(key)
                   :noindex:
                   
                   :param key: key of a DataObject to retrieve
                   
                   :return: the DataObject value, or None if not existed.
                   
                   :raise `vlcp.utils.exception.WalkKeyNotRetrieved`: raised if the key has not been retrieved from the central database yet.
                                                                      The walker function should catch this exception and stop further retrieving
                                                                      depends on the return value. ObjectDB will call `walker` again after the keys are retrieved.
                                                                      
                                                                      `WalkKeyNotRetrieved` exception is a subclass of `KeyError`
   
   :param write: A function to write a value to a key:
                
                 .. py:function:: write(key, value)
                    :noindex:
                    
                    :param key: key of a DataObject to write.
                    
                    :param value: a DataObject for updating or `None` for deleting.
                 
                 Modifed values must be written to database with `write` methods even if it is modified in-place. `write` can be used
                 on the same key for multiple times, and the last value is written when transaction ends. `walk` always retrieved
                 the last written value of a key if it has been written for at least once.

Sometimes the execution of the transaction depends on the current (server) time. with `timestamp=True`, an extra parameter `timestamp`
can be used in `walker` function:

.. py:function:: walker(walk, write, timestamp)
   :noindex:
   
   :param timestamp: A server-side timestamp in micro seconds

When a transaction needs async support (e.g. some related information are retrieved from network), an `asyncwritewalk` method can be used
instead. The `asyncwritewalk` method uses an `asyncwalker` method as a walker factory:

.. py:function:: (async) asyncwalker(last_info, container)
   :noindex:
   
   A function describing an async read-write transaction. Each time `asyncwalker` is executed, it returns a `(keys, walker)`
   tuple for a `writewalk`. the returned `walker` may raises `vlcp.utils.exception.AsyncTransactionLockException` to
   interrupt the transaction and give some extra info for the next execution, so `asyncwalker` can recreate the walker.
   
   :param last_info: When `asyncwalker` is called for the first time, last_info is `None`. After that, it is
                     the first argument of the last `AsyncTransactionLockException` raised by `walker`
   
   :param container: The routine container that executes the current routine
   
   :return: `(keys, walker)` where `keys` are the estimated keys which are needed by the transaction (for performance optimizing only).
            `walker` has the same signature used in `writewalk`, but can raise `AsyncTransactionLockException` to interrupt
            current transaction and retry from `asyncwalker`. The first argument of `AsyncTransactionLockException` will be
            passed as `last_info` when calling `asyncwalker` next time.

`writewalk` and `asyncwritewalk` has following guarantees:

1. All value retrieved by `walk` in `walker` are at the same DB version (*Consistent*)
2. Written values can only be commited to database if all the retrieved values
   are not modified by other transactions (*Isolated*)
3. Either all written values are written, or none of them are written if transaction rolls back (*Atomic*)

`writewalk` internally uses `asynctransact`, which calls lower-level `transact` repeatedly with current estimated keys. The internal updater
calls `walker` with a local cache. If the keys retrieving by `walk` is not in the current estimated keys, it is added to the list on next
try. If the `walker` completes without missing keys, the written values are returned to let the transaction finish in the KVStorage.
