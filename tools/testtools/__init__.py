import os
import unittest
import logging

from furl import furl

from redis import Redis

from majorka import Majorka

from proc import Multiprocess

from data.framework.bus import Connection as BusConnection
from data.model import ENTITIES


DEFAULT_REDIS_PORT = 8399
DEFAULT_MAJORKA_PORT = 8008
DEFAULT_REDIS_BINPATH = 'redis-server'
DEFAULT_MAJORKA_SERVER_BINPATH = '../core/target/debug/majorka'
DEFAULT_MAJORKA_CLI_BINPATH = '../core/target/debug/majorka-cli'
DEFAULT_VERBOSE = False


class EnvironmentTestCase(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.redis_port = int(os.environ.get('TEST_REDIS_PORT', DEFAULT_REDIS_PORT))
        cls.redis_binpath = os.environ.get('TEST_REDIS_BIN', DEFAULT_REDIS_BINPATH)
        cls.majorka_binpath = os.environ.get('TEST_MAJORKA_SERVER_BIN', DEFAULT_MAJORKA_SERVER_BINPATH)
        cls.majorka_port =  int(os.environ.get('TEST_MAJORKA_SERVER_PORT', DEFAULT_MAJORKA_PORT))
        cls.majorka_cli_binpath = os.environ.get('TEST_MAJORKA_CLI_BIN', DEFAULT_MAJORKA_CLI_BINPATH)
        cls.redis_url = 'redis://localhost:{port}/0'.format(port=cls.redis_port)
        cls.verbose = bool(os.environ.get('TEST_VERBOSE', DEFAULT_VERBOSE))

        cls.fixture = MajorkaFixture(binpath=cls.majorka_cli_binpath, redis_url=cls.redis_url)

        cls.redis = Redis.from_url(cls.redis_url)
        cls.bus = BusConnection(redis=cls.redis, entities_meta=ENTITIES)

        # we need a safe default behaviour, but accesing report_db may change data not as expected, see issue https://github.com/vladignatyev/majorka/issues/23
        # cls.report_db = Database(url=os.environ['TEST_CLICKHOUSE_URL'], db='test', connection_timeout=1, data_read_timeout=1)

    @classmethod
    def build_majorka_server_cmd(cls, redis_url,
                                 binpath=DEFAULT_MAJORKA_SERVER_BINPATH,
                                 listen_port=DEFAULT_MAJORKA_PORT,
                                 redis_port=DEFAULT_REDIS_PORT,
                                 verbose=False):
        return [
            binpath,
            "--redis", redis_url,
            "--port", str(listen_port),
            "--log", "debug" if verbose else "critical"
        ]

    @classmethod
    def build_redis_server_cmd(cls, binpath=DEFAULT_REDIS_BINPATH, port=DEFAULT_REDIS_PORT):
        return [binpath, "--port", str(port)]

    def setupLogger(self):
        self.logger = logging.getLogger('test')
        self.logger.setLevel(logging.DEBUG)

        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('\33[92m[%(name)s] \33[0m\33[90m%(asctime)-15s\33[1m\33[37m %(message)s\33[0m'))

        self.logger.addHandler(handler)

    def majorka_url(self):
        return furl('http://127.0.0.1:{port}/'.format(port=self.majorka_port))

    def setUp(self):
        servers = (
            ('Redis Server', EnvironmentTestCase.build_redis_server_cmd(binpath=self.redis_binpath, port=self.redis_port)),
            ('Majorka Server', EnvironmentTestCase.build_majorka_server_cmd(binpath=self.majorka_binpath, redis_url=self.redis_url, listen_port=self.majorka_port, redis_port=self.redis_port, verbose=False)),
        )

        self.setupLogger()

        self.multiprocess = Multiprocess(logger=self.logger, proc=servers)
        self.multiprocess.launch()

    def tearDown(self):
        self.multiprocess.kill()



class TrafficSimulator(object):
    def __init__(self, samples):
        # todo
        pass


class MajorkaFixture(Majorka):
    def __init__(self, *args, **kwargs):
        super(MajorkaFixture, self).__init__(*args, **kwargs)
