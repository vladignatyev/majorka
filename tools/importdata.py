from data.bus import Connection as BusConnection
from data.framework.reporting import Database
from data.framework.importing import Importing
from data.model import ENTITIES

from ipaddr import IPAddress
from decimal import Decimal

bus = BusConnection(host='localhost', port='6379', db=0)
report_db = Database(url='http://192.168.9.40:8123', db='majorka')

# data_import = Importing(bus, report_db, model_meta=ENTITIES)
# data_import.run()

# typed read
# columns = report_db.connected().get_columns_for_table("processes", db='system')
# for o, i, l in report_db.connected().read(sql="SELECT * FROM system.processes;",
#                                           columns=columns):
#     print o




for offer in bus.multiread('Offer'):
    print ""
    print "Offer"
    print offer.into_db_columns()
    print offer.into_db_row()
    break

#
for offer in bus.multiread('Campaign'):
    print ""
    print "Campaign"
    print offer.into_db_columns()
    print offer.into_db_row()
    break
#
for offer in bus.multiread('Hits', start=381):
    print ""
    print "Hits"
    print offer.into_db_columns()
    print offer.into_db_row()
    # print offer.__dict__
    break

for offer in bus.multiread('Conversions'):
    print ""
    print "Conversions"
    print offer.into_db_columns()
    print offer.into_db_row()
    break
#
# for offer in bus.multiread('Offer'):
#     print offer.__dict__


# for hit in bus.readonly().by_id("Hits:[12]").by_id("Hits:[25]").execute():
#     print hit.__dict__


# for hit in bus.multiread('Hits', start=1072):
#     print hit._idx

#
#
# report_db.drop()
#
# for entity_name in ENTITIES.keys():
#     report_db.connected().create_entity_table(entity_name)
#     for row, _, _ in report_db.connected().describe(entity_name.lower()):
#         print row
