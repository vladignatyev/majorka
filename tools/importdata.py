import logging

from data.bus import Connection as BusConnection
from data.framework.reporting import Database
from data.framework.importing import Importing
from data.model import DataImport

from ipaddr import IPAddress
from decimal import Decimal

bus = BusConnection(host='localhost', port='6379', db=0)
report_db = Database(url='http://192.168.9.38:8123', db='majorka')

data_import = DataImport(bus=bus, report_db=report_db)

data_import.load_simple_entities()
data_import.load_hits()
