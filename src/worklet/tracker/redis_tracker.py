from src.worklet.tracker.base_tracker import Tracker


class RedisTracker(Tracker):
    def __init__(self, redis_client):
        self._redis = redis_client

    def add(self, message):
        key = message.key().decode("utf-8")
        self._redis.hset("kafka_tracker", key, 1)

    def remove(self, message):
        key = message.key().decode("utf-8")
        self._redis.hdel("kafka_tracker", key)