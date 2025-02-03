import os
import redis
import logging
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger('lock_init')
logging.basicConfig(level=logging.INFO)

try:
    logger.info(f"Connecting to redis:")
    redis_client = redis.Redis(
        host=os.getenv('REDIS_HOST', 'localhost'),
        port=int(os.getenv('REDIS_PORT', 6379)),
        password=os.getenv('REDIS_PASSWORD', None),
        db=int(os.getenv('REDIS_DB', 0)),
        decode_responses=True
    )
except Exception as e:
    logger.error(f"Error: Redis connection not established: {e}", exc_info=True)


lock_keys = [
    'post_labeling_program_lock'
]

try:
    logger.info(f"Releasing locks from redis:")
    for lock_key in lock_keys:
        result = redis_client.delete(lock_key)
        if result == 1:
            logging.info(f"Lock '{lock_key}' released successfully.")
        else:
            logging.info(f"Lock '{lock_key}' does not exist or was already released.")
except Exception as e:
    logger.error(f"Error during release of locks from redis: {e}")
