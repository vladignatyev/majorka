import os
import sys
import logging

from data.framework.bus import Connection as BusConnection
from data.framework.reporting import Database
from data.model import ENTITIES, REPORTING_DB, DataImport

from ipaddr import IPAddress
from decimal import Decimal


if __name__ == '__main__':

    # sys.exit()
    redis_url = os.environ.get('REDIS_URL', None)
    clickhouse_url = os.environ.get('CLICKHOUSE_URL', None)

    if not redis_url:
        raise Exception("\n\nSet the 'REDIS_URL' environmental variable to the URL of Redis instance/slave. Example: redis://127.0.0.1:6379/1\n")
        sys.exit(1)
    if not clickhouse_url:
        raise Exception("\n\nSet the 'CLICKHOUSE_URL' environmental variable to the URL of Clickhouse instance/slave. Example: http://127.0.0.1:8123/\n")
        sys.exit(1)

    # * Setting up a logger *
    # ugly format because of handmade coloring
    logging.basicConfig(format='\33[92m[%(name)s] \33[0m\33[90m%(asctime)-15s\33[1m\33[37m %(message)s\33[0m')
    logger = logging.getLogger(DataImport.LOGGER)
    logger.setLevel(logging.DEBUG)

    logger.info("* Data import has started *")

    # * Setting up connections *
    bus = BusConnection(entities_meta=ENTITIES, url=redis_url)
    report_db = Database(url=clickhouse_url, db=REPORTING_DB)
    report_db.connected()

    logger.info("Clickhouse connected.")

    logger.info("[ Starting import ]")

    data_import = DataImport(bus=bus, report_db=report_db, logger=logger)
    data_import.load_simple_entities()
    data_import.load_hits()

    logger.info("Good bye.")
