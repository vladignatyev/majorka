import json
from redis import Redis
from model import *

class DataException(Exception):
    pass


def _check_entity(some_entity):
    return some_entity in ENTITIES_KEYS

def _get_entity_from_key(key_or_entity):
    id_chunks = key_or_entity.split(':', 1)
    return id_chunks[0]

def _check_id(some_id):
    id_chunks = some_id.split(':', 1)
    if len(id_chunks) != 2:
        raise DataException("""Provided object ID '%s' does not match the scheme of keys in Majorka storage.
        All keys should be in the form: entity-namespace:<key or [index]>""" % some_id)
    if not _check_entity(id_chunks[0]):
        raise DataException("""Object ID '%s' probably respect the scheme of keys in Majorka storage, but it has incorrect entity name.
        Provided name: %s
        Known names: %s""" % (some_id, id_chunks[0], ENTITIES.keys()))

    return some_id


def _parse_one_result(o):
    return (json.loads(o) if str(o).startswith('{') else o)

def _parse_result(*results):
    return map(lambda (o, f): f(**_parse_one_result(o)) if str(o).startswith('{') else o, results)

def _key_counter(checked_entity):
    return "%s:_counter" % checked_entity

def _key_by_index(checked_entity, index):
    return "%s:[%s]" % (checked_entity, index)


def checked_id(func):
    def wrapped(*args, **kwargs):
        _check_id(args[1])
        return func(*args, **kwargs)
    return wrapped

def checked_entity(func):
    def wrapped(*args, **kwargs):
        _check_id("%s:[%s]" % (args[1], 0))
        return func(*args, **kwargs)
    return wrapped


class AllowedQueriesPipeline(object):
    def __init__(self, redis_pipe):
        self.pipe = redis_pipe
        self.factories = []

    @checked_id
    def by_id(self, id):
        self.pipe.get(id)
        self.factories.append(ENTITIES[_get_entity_from_key(id)])
        return self

    def execute(self):
        return _parse_result(*zip(self.pipe.execute(), self.factories))


class Connection(object):
    def __init__(self, **kwargs):
        self._redis = Redis(**kwargs)

    def readonly(self):
        return AllowedQueriesPipeline(self._redis.pipeline())

    @checked_entity
    def multiread(self, entity, start_idx=0, end_idx=None):
        if start_idx < 0:
            raise Exception("Start index couldn't be less than 0.")

        checked_entity = entity

        counter = int(self._redis.get(_key_counter(checked_entity))) or 0
        last_idx = counter - 1

        if start_idx > last_idx:
            return # there is no objects to read
        if end_idx is None:
            end_idx = last_idx

        n = start_idx
        while n <= end_idx:
            obj = _parse_one_result(self._redis.get(_key_by_index(entity, n)))
            yield obj
            n += 1
