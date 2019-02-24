from data.bus import Connection as BusConnection
from data.reporting import Database

from ipaddr import IPAddress
from decimal import Decimal

# bus = BusConnection(host='localhost', port='6379', db=0)

# campaign, offer1, offer2, nonexisting, conversion = bus.readonly()\
#                                         .by_id("Campaign:[0]")\
#                                         .by_id("Offer:[0]")\
#                                         .by_id("Offer:[1]")\
#                                         .by_id("Offer:[20]")\
#                                         .by_id("Conversions:[0]")\
#                                         .execute()
#
# print conversion.revenue
# print conversion.time
# print campaign.offers
#
# print campaign.get_offers(c)
# print offer1.__dict__

# for hit in c.multiread('Hits'):
#     print hit.cost
#     print hit.time
#     print hit.destination


report_db = Database(url='http://192.168.9.40:8123', db='majorka')

from prettytable import PrettyTable
t = PrettyTable()

# for row, i, total in report_db.connected().read("""
#
#
# SELECT user, address, elapsed, memory_usage
# FROM system.processes;
# """):
#     print "%s/%s" % (i, total)
#     t.add_row(row)
#
# print t

# typed
for row, i, total in report_db.connected().read(sql="SELECT user, address, elapsed, memory_usage FROM system.processes;",
                                                columns=(('user', str), ('address', IPAddress), ('elapsed', Decimal), ('memory_usage', int))):
    print row

# non-typed
for row, i, total in report_db.connected().read(sql="SELECT user, address, elapsed, memory_usage FROM system.processes;",
                                                columns=('user', 'address', 'elapsed', 'memory_usage')):
    print row


#
# r = report_db.connected()._query('SELECT * FROM system.processes;')
# print r
# r = report_db._post_query('CREATE DATABASE IF NOT EXISTS majorka;')
# r = report_db.connected().create_database("majorka", if_not_exist=True)
# print r
# print report_db._url

# print ReportingConnection.Query.create_database('majorka', True);
