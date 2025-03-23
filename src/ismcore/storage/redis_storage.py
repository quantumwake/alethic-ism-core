from ismcore.storage.processor_state_storage import SessionStorage
from ismcore.utils.ism_logger import ism_logger, LOG_LEVEL

try:
    import redis
except ImportError:
    raise ImportError("Please install the 'redis' package via pip")

logging = ism_logger(__name__)

class RedisSessionStorage(SessionStorage):

    def __init__(self, host='localhost', port=6379, password: str = None, db=0):
        # Initialize the Redis client connection
        self.client = redis.Redis(host=host, port=port, password=password, db=db)

    # def fetch_session_list_by_user(self, session_id: str, user: str):
    #     self.client.hgetall()
    # def insert_session_message_2(self, context: SessionContext):
    #     self.client

## TODO double check this and fix it.. sessions is totally FUBAR at the moment. (SO MUCH TO DO !!! :-( NEED ENGINEERS TO CLEAN ALL THIS NIGHTMARE /)
    def insert_session_message(self, key, message):
        try:
            # Use RPUSH to append elements to the end of the list in Redis
            logging.info(f"ready to push data onto the cache {key}: {message}")
            print(f"***hello world: start*** {LOG_LEVEL}")
            self.client.rpush(key, message)
            print("***hello world: end***")
            logging.debug(f"successfully r-pushed message to list '{key}': {message}")
        except redis.RedisError as e:
            logging.error(f"error pushing message to Redis: {e}")
            print(f"***hello world: error {e}***")

    def fetch_session_list(self, key):
        try:
            # Retrieve the list stored in Redis under the specified key
            # Use the LRANGE command to get the entire list (start=0, end=-1)
            list_data = self.client.lrange(key, 0, -1)
            logging.debug(f"successfully fetched message list: '{key}")

            # Decode bytes to strings (assuming UTF-8 encoding)
            return [item.decode('utf-8') for item in list_data]
        except redis.RedisError as e:
            logging.debug(f"error pushing message to Redis: {e}")
            return []
