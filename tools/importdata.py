from data.bus import Connection as BusConnection
from data.reporting import Database

from ipaddr import IPAddress
from decimal import Decimal

report_db = Database(url='http://192.168.9.40:8123', db='majorka')
