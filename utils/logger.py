import logging

class ContextAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        if 'context' in self.extra:
            m, ka = logging.LoggerAdapter.process(self, msg, kwargs)
            return ('(%s) %s' % (self.extra['context'], m), ka)
        else:
            return logging.LoggerAdapter.process(self, msg, kwargs)
