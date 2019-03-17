import logging
import os
import sys

try:
    import psutil
except ImportError:
    print "You have to install `psutil` to launch this script: pip install psutil"
    sys.exit(1)

from proc import Multiprocess
from proc.pipelog import LogTrap

logging.basicConfig(format='\33[92m[%(name)s] \33[0m\33[90m%(asctime)-15s\33[1m\33[37m %(message)s\33[0m')
logger = logging.getLogger(Multiprocess.LOGGER)
logger.setLevel(logging.DEBUG)


DEFAULT_REDIS_PORT = 6399
DEFAULT_MAJORKA_PORT = 8008
DEFAULT_REDIS_BINPATH = 'redis-server'
DEFAULT_MAJORKA_SERVER_BINPATH = '../core/target/debug/majorka'
DEFAULT_MAJORKA_CLI_BINPATH = '../core/target/debug/majorka-cli'
DEFAULT_VERBOSE = False

redis_port = int(os.environ.get('TEST_REDIS_PORT', DEFAULT_REDIS_PORT))
redis_binpath = os.environ.get('TEST_REDIS_BIN', DEFAULT_REDIS_BINPATH)
majorka_binpath = os.environ.get('TEST_MAJORKA_SERVER_BIN', DEFAULT_MAJORKA_SERVER_BINPATH)
majorka_port =  int(os.environ.get('TEST_MAJORKA_SERVER_PORT', DEFAULT_MAJORKA_PORT))
majorka_cli_binpath = os.environ.get('TEST_MAJORKA_CLI_BIN', DEFAULT_MAJORKA_CLI_BINPATH)
verbose = bool(os.environ.get('TEST_VERBOSE', DEFAULT_VERBOSE))


def redis_url(port):
    return "redis://localhost:{port}/0".format(port=port)

def build_majorka_server_cmd(binpath=DEFAULT_MAJORKA_SERVER_BINPATH, listen_port=DEFAULT_MAJORKA_PORT, redis_port=DEFAULT_REDIS_PORT, verbose=False):
    return [
        binpath,
        "--redis", redis_url(redis_port),
        "--port", str(listen_port),
        "--log", "debug" if verbose else "critical"
    ]

def build_redis_server_cmd(binpath=DEFAULT_REDIS_BINPATH, port=DEFAULT_REDIS_PORT):
    return [binpath, "--port", str(port)]


servers = (
    ('Redis Server', build_redis_server_cmd(binpath=redis_binpath, port=redis_port)),
    ('Majorka Server', build_majorka_server_cmd(binpath=majorka_binpath, listen_port=majorka_port, redis_port=redis_port, verbose=False)),
)


class TestCampaignSetup(object):
    def __init__(self, majorka_cli_binpath=DEFAULT_MAJORKA_CLI_BINPATH,
                 redis_port=DEFAULT_REDIS_PORT, num_offers=4):
        self._cli = majorka_cli_binpath
        self._redis_url = redis_url(redis_port)

    pass



with Multiprocess(logger=logger, proc=servers) as s:
    s.launch()
    redis_log_reader = s.proc_loggers['Redis Server'].get_reader()
    majorka_log_reader = s.proc_loggers['Majorka Server'].get_reader()

    while True: # main loop
        died = s.poll()
        if died is not None:
            break

        yn = raw_input("Kill them all and exit?\n")
        if yn.strip().lower() == 'y':
            break
        else:
            redis_logs = redis_log_reader.read()
            if redis_logs:
                logger.info("Redis log updated:\n\n\t" + '\t'.join(redis_logs))

            majorka_logs = majorka_log_reader.read()
            if majorka_logs:
                logger.info("Majorka log updated:\n\n\t" + '\t'.join(majorka_logs))

    # redis_log.destroy()
