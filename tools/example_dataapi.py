from data.bus import Connection as BusConnection
from data.reporting import Database

from prettytable import PrettyTable

from ipaddr import IPAddress
from decimal import Decimal

# Databus reading example
############################################
bus = BusConnection(host='localhost', port='6379', db=0)

campaign, offer1, offer2, nonexisting, conversion = bus.readonly()\
                                        .by_id("Campaign:[0]")\
                                        .by_id("Offer:[0]")\
                                        .by_id("Offer:[1]")\
                                        .by_id("Offer:[20]")\
                                        .by_id("Conversions:[0]")\
                                        .execute()
assert nonexisting is None
print campaign.offers
print campaign.get_offers(c)

print conversion.revenue
print conversion.time

print offer1.__dict__


for hit in c.multiread('Hits'):
    print hit.cost
    print hit.time
    print hit.destination

# Multiline SQL read from Reporting Database
############################################
report_db = Database(url='http://192.168.9.40:8123', db='majorka')

t = PrettyTable()
for row, i, total in report_db.connected().read("""


SELECT user, address, elapsed, memory_usage
FROM system.processes;
"""):
    print "%s/%s" % (i, total)
    t.add_row(row)

print t

# Typed SQL query
############################################
for row, i, total in report_db.connected().read(sql="SELECT user, address, elapsed, memory_usage FROM system.processes;",
                                                columns=(('user', str), ('address', IPAddress), ('elapsed', Decimal), ('memory_usage', int))):
    print row

# Non-typed SQL query
############################################
for row, i, total in report_db.connected().read(sql="SELECT user, address, elapsed, memory_usage FROM system.processes;",
                                                columns=('user', 'address', 'elapsed', 'memory_usage')):
    print row
