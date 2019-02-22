from data.bus import Connection

c = Connection(host='localhost', port='6379', db=0)
campaign, offer1, offer2, nonexisting, conversion = c.readonly()\
                                        .by_id("Campaign:[0]")\
                                        .by_id("Offer:[0]")\
                                        .by_id("Offer:[1]")\
                                        .by_id("Offer:[20]")\
                                        .by_id("Conversions:[0]")\
                                        .execute()

print conversion.revenue
print conversion.time
print campaign.offers
#
# print campaign.get_offers(c)
# print offer1.__dict__
#
# # print nonexisting
# #
# # import timeit
# # start = timeit.timeit()
for hit in c.multiread('Hits'):
    # print hit.__dict__
    print hit.cost
    print hit.time
    print hit.destination
    break
# # end = timeit.timeit()
# # print end - start
