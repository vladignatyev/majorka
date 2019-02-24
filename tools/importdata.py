from data.bus import Connection as BusConnection
from data.reporting import Database
from data.model import ENTITIES

from ipaddr import IPAddress
from decimal import Decimal

bus = BusConnection(host='localhost', port='6379', db=0)
report_db = Database(url='http://192.168.9.40:8123', db='majorka')

# for row, _, _ in report_db.connected().describe("processes", db='system'):
#     print row


# for offer in bus.multiread('Offer'):
#     print offer.__dict__

# for offer in bus.multiread('Offer'):
#     print offer.__dict__


# for hit in bus.readonly().by_id("Hits:[12]").by_id("Hits:[25]").execute():
#     print hit.__dict__


for hit in bus.multiread('Hits', start=1072):
    print hit._idx
#

#
#
# report_db.drop()
#
# for entity_name in ENTITIES.keys():
#     report_db.connected().create_entity_table(entity_name)
#     for row, _, _ in report_db.connected().describe(entity_name.lower()):
#         print row
