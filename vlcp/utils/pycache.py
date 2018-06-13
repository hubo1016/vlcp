'''
Created on 2015/10/19

:author: hubo

Remove pycache files from a module, to ensure a successful reload
'''
import os

try:
    reload
except Exception:
    try:
        from importlib import reload
    except Exception:
        from imp import reload

def remove_cache(module):
    if hasattr(module, '__cached__'):
        try:
            os.remove(module.__cached__)
        except Exception:
            pass
    else:
        f = module.__file__
        try:
            if f.endswith('.pyc'):
                os.remove(f)
            elif f.endswith('.py'):
                os.remove(f + 'c')
        except Exception:
            pass

removeCache = remove_cache
