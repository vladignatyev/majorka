from data.framework.types import *
from data.framework.reporting import Database

d = Database(url='http://157.230.19.15', db='test')

print d.connected()

# columns = d.get_columns_for_table('processes', db='system')

# print list(d.read(sql="SELECT * FROM system.processes;", columns=columns))

# print d.write(sql="""
# CREATE TABLE IF NOT EXISTS test.sampledata
# (
#     id UInt64,
#     date_added Date DEFAULT today(),
#     payload String
# ) ENGINE = MergeTree(date_added, (id, date_added), 8192)
# """)


# d.write(sql="INSERT INTO test.sampledata (id, payload) FORMAT TabSeparated\n1\tSomestring")

from prettytable import PrettyTable

pt = PrettyTable()

# columns = d.get_columns_for_table('sampledata')
columns = filter(lambda (column_name, column_type): column_name in ['user', 'address', 'elapsed', 'memory_usage'], d.get_columns_for_table('processes', db='system'))

header = zip(*columns)[0]
pt.field_names = header

for row, i, total in (d.read(sql="SELECT user, address, elapsed, memory_usage FROM system.processes", columns=columns)):
    pt.add_row(map(lambda k: row[k], header))
    # print row
#
print pt
