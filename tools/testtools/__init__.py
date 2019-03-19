import os
import unittest
import logging

from furl import furl

from time import sleep
from redis import Redis, ConnectionError

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


global HANG_TEST
HANG_TEST = False


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

        # todo: extract to Environment
        cls.setupLogger()
        cls.setupServers()
        cls.setupConnections()

        # we need a safe default behaviour, but accesing report_db may change data not as expected, see issue https://github.com/vladignatyev/majorka/issues/23
        # cls.report_db = Database(url=os.environ['TEST_CLICKHOUSE_URL'], db='test', connection_timeout=1, data_read_timeout=1)

    @classmethod
    def tearDownClass(cls):
        cls.multiprocess.kill()

    @classmethod
    def setupLogger(cls):
        cls.logger = logging.getLogger('test')
        cls.logger.setLevel(logging.DEBUG)

        handler = logging.StreamHandler()
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter('\33[92m[%(name)s] \33[0m\33[90m%(asctime)-15s\33[1m\33[37m %(message)s\33[0m'))

        cls.logger.addHandler(handler)

    @classmethod
    def setupConnections(cls):
        cls.redis = Redis.from_url(cls.redis_url)
        cls.bus = BusConnection(redis=cls.redis, entities_meta=ENTITIES)

    @classmethod
    def setupServers(cls):
        servers = (
            ('Redis Server', EnvironmentTestCase.build_redis_server_cmd(binpath=cls.redis_binpath, port=cls.redis_port)),
            ('Majorka Server', EnvironmentTestCase.build_majorka_server_cmd(binpath=cls.majorka_binpath, redis_url=cls.redis_url, listen_port=cls.majorka_port, redis_port=cls.redis_port, verbose=False)),
        )

        cls.multiprocess = Multiprocess(logger=cls.logger, proc=servers)
        cls.multiprocess.launch()

        cls.multiprocess.await_socket(proc='Redis Server', port=cls.redis_port)
        cls.multiprocess.await_socket(proc='Majorka Server', port=cls.majorka_port)

        cls.log_readers = dict(map(lambda (k, l): (k, l.get_reader()), cls.multiprocess.proc_loggers.items()))

    @classmethod
    def build_majorka_server_cmd(cls, redis_url,
                                 binpath=DEFAULT_MAJORKA_SERVER_BINPATH,
                                 listen_port=DEFAULT_MAJORKA_PORT,
                                 redis_port=DEFAULT_REDIS_PORT,
                                 verbose=True):
        return [
            binpath,
            "--redis", redis_url,
            "--port", str(listen_port),
            "--log", "debug" if verbose else "critical"
        ]

    @classmethod
    def build_redis_server_cmd(cls, binpath=DEFAULT_REDIS_BINPATH, port=DEFAULT_REDIS_PORT):
        return [binpath, "--port", str(port)]

    def majorka_url(self):
        return furl('http://127.0.0.1:{port}/'.format(port=self.majorka_port))

    def setUp(self):
        self.redis.flushdb()

    def tearDown(self):
        self.redis.flushdb()


def hang(test):
    """
    Test methods decorator that makes them hangs after execution if `--hang`
    option provided.
    You should use `testtools.main()` instead of `unittest.main()` to make this
    works or set global `HANG_TEST` to True before `unittest.main()`
    """
    def wrapper(self):
        test(self)

        global HANG_TEST

        while HANG_TEST and True:
            for k, reader in self.log_readers.items():
                new_logs = reader.read()
                if new_logs:
                    self.logger.debug("New logs from process `%s`\n\n\t", k)
                    self.logger.debug('\t'.join(new_logs))

            if raw_input("Stop hanging? [Y/n]: ").lower() == 'y':
                break

    # Wrapped method should have the same name,
    # otherwise it won't run!
    #
    # See discussion: https://stackoverflow.com/questions/6312167/python-unittest-cant-call-decorated-test
    wrapper.__name__ = test.__name__
    return wrapper


def main(*args, **kwargs):
    import sys
    global HANG_TEST

    argv = sys.argv

    if '--hang' in argv:
        i = argv.index('--hang')
        argv = argv[:i] + argv[i + 1:]
        HANG_TEST = True

    unittest.main(argv=argv, *args, **kwargs)


class MajorkaFixture(Majorka):
    def __init__(self, *args, **kwargs):
        super(MajorkaFixture, self).__init__(*args, **kwargs)
