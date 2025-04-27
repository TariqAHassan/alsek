"""

    Singletons

"""

from alsek import Broker
from alsek.storage.backends.redis.standard import RedisBackend

backend = RedisBackend()
broker = Broker(backend)
