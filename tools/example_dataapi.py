from data.bus import Connection as BusConnection
from data.reporting import Database

from prettytable import PrettyTable

from ipaddr import IPAddress
from decimal import Decimal

# Databus reading example
############################################
bus = BusConnection(url='redis://localhost:6379/1')

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


for hit in bus.multiread('Hits'):
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


# Typed read of unknown table structure
#######################################

# columns is a ((name1, type1), (name2, type2) ...) tuple
columns = report_db.connected().get_columns_for_table("processes", db='system')

for o, i, l in report_db.connected().read(sql="SELECT * FROM system.processes;",
                                          columns=columns):
    print o
