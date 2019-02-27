from data.model import ENTITIES
from data.framework.bus import Connection

bus = Connection(url='redis://localhost:6380/0', entities_meta=ENTITIES)

hits_table = [['id', 'cost', 'time', 'destination', 'campaign', 'external_id']]
conversions_table = [['id', 'revenue', 'time', 'external_id']]

c = 0
for hit in bus.multiread('Hits'):
    c +=1
    print c
    cost = hit.cost
    hits_table += [[hit._idx, str(float(cost[0])), hit.time, hit.destination_id, hit.campaign_id, hit.dimensions[u'external_id'],]]

c = 0
for conversion in bus.multiread('Conversions'):
    c +=1
    print c
    conversions_table += [[conversion._idx, str(conversion.revenue[0]), conversion.time, conversion.external_id]]


with open('hits.csv', 'wb') as f:
    for row in hits_table:
        f.write("%s\n" % ';'.join(map(lambda k: str(k), row)))


with open('conversions.csv', 'wb') as f:
    for row in conversions_table:
        f.write("%s\n" % ';'.join(map(lambda k: str(k), row)))
