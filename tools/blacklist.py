from blacklist import *

def execute():
    from data.framework.reporting import Database

    d = Database(url='http://157.230.19.15/', db='majorka')

    slice_blacklist = SliceBlacklist(campaign_id=0, min_clicks=1, acceptable_roi=15)
    final_sql = slice_blacklist.final_sql()

    columns = d.describe_query(final_sql)

    print 'columns', columns

    items = d.read(sql=final_sql, columns=columns)

    print 'items', items

    zones_black = []
    for o, i, l in items:
        print 'o - {o}, i - {i}, l - {l}'.format(o = o, i = i, l = l)
        zones_black.append(o['Site'])
    print zones_black

if __name__ == '__main__':
    execute()
