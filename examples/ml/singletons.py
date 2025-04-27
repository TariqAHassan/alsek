"""

    Singletons

"""

from alsek import Broker
from alsek.storage.backends.redis.standard import RedisBackend
from alsek.storage.result import ResultStore

backend = RedisBackend()
broker = Broker(backend)
result_store = ResultStore(backend)
