'''
Generate fixture from Redis data

Usage example: TEST_REDIS_URL=redis://localhost:6379/0 python framework/test/_prepare_bus_fixture.py  > framework/test/redis_fixture.py
'''
import os
import sys

from redis import Redis
from redis.exceptions import ResponseError

def escape(s): return s.replace("'", "\\'")


connection = Redis.from_url(os.environ['TEST_REDIS_URL'])

key_values = {}
for key in connection.scan_iter():
    try:
        sys.stderr.write("Traversing key: %s\n" % key)
        key_values[key] = connection.get(key)
    except ResponseError:
        sys.stderr.write("...wrong type, skipping.\n")


sys.stdout.write("fixture_data = {}\n")
sys.stdout.write('\n'.join(map(lambda k: "fixture_data['%s'] = '%s'" % (k, escape(key_values[k])), sorted(key_values.keys()))))
sys.stdout.flush()
