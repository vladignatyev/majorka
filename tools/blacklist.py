from blacklist import *
import click

@click.command()
@click.option('--ch-url', envvar='CH_URL', required=True, help='Clickhouse URL, i.e. http://192.168.9.39:8123/. You can use CH_URL environmental variable to set this parameter.')
@click.option('--info', help='Print blacklist for given campaign')
@click.option('--campaign', type=click.IntRange(min=0, max=100000), required=True, help='Id for campaign to print blacklist for')
@click.option('--roi', required=True, type=click.IntRange(min=-100, max=100000), help='minimum ROI for particular slice ( zoneid -> connection_type -> offer)')
@click.option('--clicks', required=True, type=click.IntRange(min=0, max=10000000000L), help='minimum clicks for every slice required to decide that zone will be included in blacklist')
def execute(ch_url, info, campaign, roi, clicks):
    """Prints blacklist for given campaign"""
    from data.framework.reporting import Database

    d = Database(url=ch_url, db='majorka')

    slice_blacklist = SliceBlacklist(campaign_id=long(campaign), min_clicks=int(clicks), acceptable_roi=int(roi))
    final_sql = slice_blacklist.final_sql()

    columns = d.describe_query(final_sql)

    items = d.read(sql=final_sql, columns=columns)

    print "\n".join(map(lambda (o, i, l): o['Site'], items))

if __name__ == '__main__':
    execute()
