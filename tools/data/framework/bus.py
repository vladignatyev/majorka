import sys
import json
from redis import Redis


class ConnectionError(Exception): pass
class DataException(Exception): pass


def _check_entity(entities_meta, some_entity):
    return some_entity in entities_meta.keys()

def _get_entity_from_key(key_or_entity):
    id_chunks = key_or_entity.split(':', 1)
    return id_chunks[0]

def _check_id(entities_meta, some_id):
    id_chunks = some_id.split(':', 1)
    if len(id_chunks) != 2:
        raise DataException("""Provided object ID '%s' does not match the scheme of keys in Majorka storage.
        All keys should be in the form: entity-namespace:<key or [index]>""" % some_id)
    if not _check_entity(entities_meta, id_chunks[0]):
        raise DataException("""Object ID '%s' probably respect the scheme of keys in Majorka storage, but it has incorrect entity name.
        Provided name: %s
        Known names: %s""" % (some_id, id_chunks[0], entities_meta.keys()))

    return some_id


def _key_counter(checked_entity):
    return "%s:_counter" % checked_entity

def _key_by_index(checked_entity, index):
    return "%s:[%s]" % (checked_entity, index)


def checked_id(func):
    def wrapped(self, *args, **kwargs):
        _check_id(self._entities_meta, args[0])
        return func(self, *args, **kwargs)
    return wrapped

def checked_entity(func):
    def wrapped(self, *args, **kwargs):
        _check_id(self._entities_meta, "%s:[%s]" % (args[0], 0))
        return func(self, *args, **kwargs)
    return wrapped


def _parse_result(connection, *results):
    return map(lambda (i, o, f): f(connection, i, **json.loads(o)) if str(o).startswith('{') else o, results)


class AllowedQueriesPipeline(object):
    def __init__(self, connection, redis_pipe, entities_meta):
        self._connection = connection
        self._pipe = redis_pipe
        self._factories = []
        self._ids = []
        self._entities_meta = entities_meta

    @checked_id
    def by_id(self, id):
        self._pipe.get(id)
        self._factories.append(self._entities_meta[_get_entity_from_key(id)])
        self._ids.append(id)
        return self

    def execute(self):
        return _parse_result(self._connection, *zip(self._ids, self._pipe.execute(), self._factories))


class Connection(object):
    def __init__(self, entities_meta, url=None, redis=None):
        if url:
            self._redis = Redis.from_url(url)
        elif redis:
            self._redis = redis
        else:
            raise ConnectionError("Neither `url` nor `redis` parameters provided.")
        self._entities_meta = entities_meta

    def readonly(self):
        return AllowedQueriesPipeline(self, self._redis.pipeline(), self._entities_meta)

    @checked_entity
    def multiread(self, entity, start=0, end=None):
        if start < 0:
            raise Exception("Start index couldn't be less than 0.")

        checked_entity = entity

        counter = int(self._redis.get(_key_counter(checked_entity))) or 0
        last_idx = counter - 1

        if start > last_idx:
            return # there is no objects to read, returning empty iterator
        if end is None:
            end = last_idx

        n = start
        while n <= end:
            obj = _parse_result(self, (_key_by_index(entity, n), self._redis.get(_key_by_index(entity, n)), self._entities_meta[entity]))[0]
            yield obj
            n += 1
