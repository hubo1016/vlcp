'''
Created on 2018/7/2

:author: hubo
'''
class ModuleAPICallTimeoutException(Exception):
    pass


class APIRejectedException(Exception):
    pass


class StaleResultException(Exception):
    def __init__(self, result, desc = "Result is stale"):
        Exception.__init__(self, desc)
        self.result = result


class AsyncTransactionLockException(Exception):
    def __init__(self, info = None):
        Exception.__init__(self)
        self.info = info


class TransactionFailedException(Exception):
    pass


class TransactionRetryExceededException(TransactionFailedException):
    def __init__(self):
        TransactionFailedException.__init__(self, "Max retry exceeded")


class TransactionTimeoutException(TransactionFailedException):
    def __init__(self):
        TransactionFailedException.__init__(self, "Timeout exceeded")


class WalkKeyNotRetrieved(KeyError):
    pass
