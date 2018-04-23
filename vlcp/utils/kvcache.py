'''
Created on 2018/4/17

:author: hubo
'''

class KVCache(object):
    def __init__(self):
        self._cache = {}
    
    def get(self, key, default = None):
        return self._cache.get(key, default)
    
    def set(self, key, value):
        self._cache[key] = value
    
    def update(self, key, oper, *args, **kwargs):
        result, new_value = oper(key, self._cache.get(key), *args, **kwargs)
        if new_value is not None:
            self._cache[key] = new_value
        return result
    
    def gc(self, keys):
        if len(self._cache) > len(keys) * 1.2:
            old_cache = self._cache
            self._cache = {k: old_cache[k] for k in keys if k in old_cache}
