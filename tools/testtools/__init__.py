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


class MajorkaInterface(object):
    def __init__(self, listen_port):
        self.majorka_port = listen_port

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

    def majorka_url(self, campaign_alias=None):
        """
        Majorka server URL builder based on `furl`

        >>> i = MajorkaInterface(listen_port=8008)
        >>> assert str(i.majorka_url()) == 'http://127.0.0.1:8008/'
        >>> assert str(i.majorka_url(campaign_alias='testcampaign')) == 'http://127.0.0.1:8008/alias?currency=%7Bcurrency%7D&cost=%7Bcost%7D&zone=%7Bzone%7D'
        >>> assert str(i.majorka_url(campaign_alias='testcampaign').add({'myparam': '{myparam}'})) == 'http://127.0.0.1:8008/alias?currency=%7Bcurrency%7D&cost=%7Bcost%7D&zone=%7Bzone%7D&myparam=%7Bmyparam%7D'
        """
        if campaign_alias is None:
            return furl('http://127.0.0.1:{port}/'.format(port=self.majorka_port))
        else:
            return furl('http://127.0.0.1:{port}/'.format(port=self.majorka_port)).join('alias').add({
                'zone': '{zone}',
                'cost': '{cost}',
                'currency': '{currency}'
            })


class EnvironmentTestCase(unittest.TestCase):
    def setupLogger(self):
        self.logger = logging.getLogger('test')
        self.logger.setLevel(logging.DEBUG)

        if not self.logger.handlers:
            handler = logging.StreamHandler()
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(logging.Formatter('\33[92m[%(name)s] \33[0m\33[90m%(asctime)-15s\33[1m\33[37m %(message)s\33[0m'))

            self.logger.addHandler(handler)

    def setupConnections(self):
        self.redis = Redis.from_url(self.redis_url)
        self.bus = BusConnection(redis=self.redis, entities_meta=ENTITIES)

    def setupServers(self):
        servers = (
            ('Redis Server', self.majorka.build_redis_server_cmd(binpath=self.redis_binpath, port=self.redis_port)),
            ('Majorka Server', self.majorka.build_majorka_server_cmd(binpath=self.majorka_binpath, redis_url=self.redis_url, listen_port=self.majorka_port, redis_port=self.redis_port, verbose=False)),
        )

        self.multiprocess = Multiprocess(logger=self.logger, proc=servers)
        self.multiprocess.launch()

        self.multiprocess.await_socket(proc='Redis Server', port=self.redis_port)
        self.multiprocess.await_socket(proc='Majorka Server', port=self.majorka_port)

        self.log_readers = dict(map(lambda (k, l): (k, l.get_reader()), self.multiprocess.proc_loggers.items()))
        #
        # import signal
        #
        # def my_ctrlc_handler(signal, frame):
        #     self.multiprocess.kill()
        #     sys.exit(0)
        #     # raise KeyboardInterrupt
        #
        # signal.signal(signal.SIGINT, my_ctrlc_handler)


    def setUp(self):
        self.redis_port = int(os.environ.get('TEST_REDIS_PORT', DEFAULT_REDIS_PORT))
        self.redis_binpath = os.environ.get('TEST_REDIS_BIN', DEFAULT_REDIS_BINPATH)
        self.majorka_binpath = os.environ.get('TEST_MAJORKA_SERVER_BIN', DEFAULT_MAJORKA_SERVER_BINPATH)
        self.majorka_port =  int(os.environ.get('TEST_MAJORKA_SERVER_PORT', DEFAULT_MAJORKA_PORT))
        self.majorka_cli_binpath = os.environ.get('TEST_MAJORKA_CLI_BIN', DEFAULT_MAJORKA_CLI_BINPATH)
        self.redis_url = 'redis://localhost:{port}/0'.format(port=self.redis_port)
        self.verbose = bool(os.environ.get('TEST_VERBOSE', DEFAULT_VERBOSE))

        self.fixture = MajorkaFixture(binpath=self.majorka_cli_binpath, redis_url=self.redis_url)

        self.majorka = MajorkaInterface(listen_port=self.majorka_port)

        # todo: extract to Environment
        self.setupLogger()
        self.setupServers()
        self.setupConnections()

        # we need a safe default behaviour, but accesing report_db may change data not as expected, see issue https://github.com/vladignatyev/majorka/issues/23
        # self.report_db = Database(url=os.environ['TEST_CLICKHOUSE_URL'], db='test', connection_timeout=1, data_read_timeout=1)

        self.redis.flushdb()



    def tearDown(self):
        self.redis.flushdb()

        self.multiprocess.kill()

    def __exit__(self, exception_type, exception_val, trace):
        super(EnvironmentTestCase, self).__exit__(exception_type, exception_val, trace)
        return False # propagate exceptions



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

    unittest.main(argv=argv, catchbreak=False, *args, **kwargs)


class MajorkaFixture(Majorka):
    def __init__(self, *args, **kwargs):
        super(MajorkaFixture, self).__init__(*args, **kwargs)


if __name__ == '__main__':
    # Run doctests of the module
    import doctest
    doctest.testmod()
