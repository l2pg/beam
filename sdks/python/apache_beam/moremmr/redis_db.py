import redis
import time
from data_collector import log_error_message, log_metric_value, log_info

class RedisDb:

    def __init__(self):
        self.redis_db_host = 'moremmr-in-parsing.redis.cache.windows.net'
        self.redis_db_port = 6379
        self.redis_db_password = 'fW1DevVVbnSlUZREntQ4MqPTvwG1Yq5YPWdgHfdBiuU='
        self.redis_db = None

        self.timer1 = int(time.time())
        self.timer1_refresh_timeout = 180 # 3 min

    def connect_to_redis_db(self, reconnect=False):
        timer1_offset = int(time.time())
        if timer1_offset - self.timer1 >= self.timer1_refresh_timeout:
            reconnect = True

        if type(self.redis_db).__name__ != 'StrictRedis' or reconnect:
            pool = redis.ConnectionPool(host=self.redis_db_host,
                                            port=self.redis_db_port,
                                            db=0,
                                            password=self.redis_db_password,
                                            encoding='utf-8',
                                            decode_responses=True,
                                            socket_timeout=5,
                                            socket_connect_timeout=5,
                                            retry_on_timeout=False
                                            )

            self.redis_db = redis.StrictRedis(connection_pool=pool)

        try:
            result = self.redis_db.ping()
        except Exception as ex:
            result = False

        return result

    def __del__(self):
        del self.redis_db

    def set_expire(self, name, expiration_seconds = 0):
        result = False

        if expiration_seconds > 0:
            if self.connect_to_redis_db():
                try:
                    self.redis_db.expire(name, expiration_seconds)
                    result = True
                except Exception as ex:
                    err_msg = 'Failed Redis_db set_expire: {0}'.format(ex)
                    log_error_message(err_msg)

        return result

    def get_hash(self, name, attribute):
        result = None

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.hget(name, attribute)
            except Exception as ex:
                err_msg = 'Failed Redis_db get_hash: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def get_hash_all(self, name):
        result = {}

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.hgetall(name)
            except Exception as ex:
                err_msg = 'Failed Redis_db get_hash_all: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def set_hash(self, name, attribute, value, expiration_seconds=0):
        result = None

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.hset(name, attribute, value)
                self.set_expire(name, expiration_seconds)
            except Exception as ex:
                err_msg = 'Failed Redis_db set_hash: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def del_hash_attribute(self, name, attribute):
        result = False

        if self.connect_to_redis_db():
            try:
                self.redis_db.hdel(name, attribute)
                result = True
            except Exception as ex:
                err_msg = 'Failed Redis_db del_hash_attribute: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def queue_push(self, name, value):
        result = False

        if self.connect_to_redis_db():
            try:
                self.redis_db.rpush(name, value)
                result = True
            except Exception as ex:
                err_msg = 'Failed Redis_db queue_push: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def queue_pop(self, name):
        result = None

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.lpop(name)
            except Exception as ex:
                err_msg = 'Failed Redis_db queue_pop: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def increment(self, name, value, expiration_seconds = 0):
        result = None

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.incr(name, value)
                self.set_expire(name, expiration_seconds)
            except Exception as ex:
                err_msg = 'Failed Redis_db increment: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def get_value(self, name, default = None):
        result = None

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.get(name)
                if result is None:
                    result = default
            except Exception as ex:
                err_msg = 'Failed Redis_db get_value: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def set_value(self, name, value, expiration_seconds = 0):
        result = None

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.set(name, value)
                self.set_expire(name, expiration_seconds)
            except Exception as ex:
                err_msg = 'Failed Redis_db set_value: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def delete_value(self, key):
        result = False

        if self.connect_to_redis_db():
            try:
                self.redis_db.delete(key)
                result = True
            except Exception as ex:
                err_msg = 'Failed Redis_db delete_value: {0}'.format(ex)
                log_error_message(err_msg)
                result = False

        return result

    def get_len(self, name):
        result = None

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.llen(name)
            except Exception as ex:
                err_msg = 'Failed Redis_db get_len: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def exists(self, name):
        result = False

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.exists(name)
            except Exception as ex:
                err_msg = 'Failed Redis_db exists: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def get_list_of_set_values(self, name):
        values_list = []

        if self.connect_to_redis_db():
            try:
                members = self.redis_db.smembers(name)
                for member in members:
                    if member is not None:
                        values_list.append(member)
            except Exception as ex:
                err_msg = 'Failed Redis_db get_list_of_set_values: {0}'.format(ex)
                log_error_message(err_msg)

        return values_list

    def put_list_to_set(self, name, values_list = [], expiration_seconds=0):
        result = False

        if self.connect_to_redis_db():
            try:
                for value in values_list:
                    self.redis_db.sadd(name, value)

                if values_list:
                    self.set_expire(name, expiration_seconds)

                result = True
            except Exception as ex:
                err_msg = 'Failed Redis_db put_list_to_set: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def pop_random_values_from_set(self, name, values_count = 0):
        values_list = []

        if self.connect_to_redis_db():
            try:
                for i in range(0, values_count):
                    set_value = self.redis_db.spop(name)
                    if set_value is not None:
                        values_list.append(int(set_value))
            except Exception as ex:
                err_msg = 'Failed Redis_db pop_random_values_from_set: {0}'.format(ex)
                log_error_message(err_msg)

        return values_list

    def delete_value_from_set(self, name, value):
        result = False

        if self.connect_to_redis_db():
            try:
                self.redis_db.srem(name, value)
                result = True
            except Exception as ex:
                err_msg = 'Failed Redis_db delete_value_from_set: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def get_set_cardinality(self, name):
        result = 0

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.scard(name)
            except Exception as ex:
                err_msg = 'Failed Redis_db get_set_cardinality: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def get_pipeline(self):
        result = None

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.pipeline()
            except Exception as ex:
                err_msg = 'Failed Redis_db get_pipeline: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def get_all_hash_attributes_values(self, name):
        result = None

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.hscan(name)
            except Exception as ex:
                err_msg = 'Failed Redis_db get_all_hash_attributes_values: {0}'.format(ex)
                log_error_message(err_msg)

        return result

    def get_ttl(self, name):
        result = -2

        if self.connect_to_redis_db():
            try:
                result = self.redis_db.ttl(name)
            except Exception as ex:
                err_msg = 'Failed Redis_db get_ttl: {0}'.format(ex)
                log_error_message(err_msg)

        return result
