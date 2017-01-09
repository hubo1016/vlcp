'''
Created on 2016/2/22

OVSDB JSON structure helpers. Write less!

These are just simple wrappers, see https://tools.ietf.org/html/rfc7047#page-28 for details

:author: hubo
'''
from vlcp.protocol.jsonrpc import JsonRPCNotificationEvent

def list_dbs():
    return ("list_dbs", [])

def get_schema(dbname):
    return ("get_schema", [dbname])

def transact(dbname, *operations):
    return ("transact", [dbname] + list(operations))

def cancel(msgid):
    return ("cancel", [msgid])

def monitor(dbname, monitorid, monitorrequests):
    return ("monitor", [dbname, monitorid, monitorrequests])

def monitor_matcher(connection, monitorid):
    return JsonRPCNotificationEvent.createMatcher(
                'update', connection, connection.connmark,
                _ismatch = lambda x: x.params[0] == monitorid)

def monitor_cancel(monitorid):
    return ("monitor_cancel", [monitorid])

def lock(lockid):
    return ("lock", [lockid])

def steal(lockid):
    return ("steal", [lockid])

def unlock(lockid):
    return ("unlock", [lockid])

def insert(table, row, uuid_name = None):
    if uuid_name is not None:
        return {"op":"insert", "table":table, "row":row, "uuid-name":uuid_name}
    else:
        return {"op":"insert", "table":table, "row":row}

def select(table, where, columns = None):
    if columns is not None:
        return {"op": "select", "table": table, "where": where, "columns": columns}
    else:
        return {"op": "select", "table": table, "where": where}

def update(table, where, row):
    return {"op": "update", "table": table, "where": where, "row": row}

def mutate(table, where, mutations):
    return {"op": "mutate", "table": table, "where": where, "mutations": mutations}

def delete(table, where):
    return {"op": "delete", "table": table, "where": where}

def wait(table, where, columns, rows, untilequal = True, timeout = None):
    if timeout is None:
        return {"op": "wait", "table": table, "where": where, "columns": columns,
            "until": "==" if untilequal else "!=", "rows": rows}
    else:
        return {"op": "wait", "table": table, "where": where, "columns": columns,
            "until": "==" if untilequal else "!=", "rows": rows, "timeout": timeout}

def commit(durable = True):
    return {"op": "commit", "durable": durable}

def abort():
    return {"op": "abort"}

def comment(comment):
    return {"op": "comment", "comment": comment}

def assert_lock(lockid):
    return {"op": "assert", "lock": lockid}

def oset(*objs):
    if len(objs) == 1:
        return objs[0]
    else:
        return ["set", list(objs)]

def omap(*pairs, **kwargs):
    return ["map", [list(p) for p in pairs] + [[k,v] for k,v in kwargs.items()]]

def pair(key, value):
    return [key, value]

def uuid(uuid):
    return ["uuid", uuid]

def named_uuid(name):
    return ["named-uuid", name]

def condition(column, function, value):
    return [column, function, value]

def mutation(column, mutator, value):
    return [column, mutator, value]

def monitor_request(columns = None, initial = None, insert = None, delete = None, modify = None):
    ro = {}
    if columns is not None:
        ro['columns'] = columns
    s = {}
    if initial is not None:
        s['initial'] = initial
    if insert is not None:
        s['insert'] = insert
    if delete is not None:
        s['delete'] = delete
    if modify is not None:
        s['modify'] = modify
    if s:
        ro['select'] = s
    return ro

def omap_getvalue(omap, key):
    for k,v in omap[1]:
        if k == key:
            return v
    return None

def getlist(oset):
    if isinstance(oset, list) and oset[0] == "set":
        return oset[1]
    else:
        return [oset]

def getdict(omap):
    return dict(omap[1])

def getoptional(obj):
    if obj == ["set", []]:
        return None
    else:
        return obj
