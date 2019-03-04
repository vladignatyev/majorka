fixture_data = {}



# Campaigns
fixture_data['Campaign:_counter'] = '1'
fixture_data['Campaign:[0]'] = '{"name":"testcampaign","alias":"majorka","offers":["Offer:[0]","Offer:[1]","Offer:[2]"],"paused_offers":[],"optimize":true,"optimization_paused":false,"hit_limit_for_optimization":30,"slicing_attrs":["zone","connection_type"]}'
fixture_data['Campaign:by_alias:majorka'] = 'Campaign:[0]'

# Offers
fixture_data['Offer:_counter'] = '3'
fixture_data['Offer:[0]'] = '{"name":"Video Player - Big Play","url_template":"https://jaunithuw.com/?h=9dad9c9097a736ce162988dc28d0dda60810115f&pci={external_id}&ppi={zone}"}'
fixture_data['Offer:[1]'] = '{"name":"18+ Tap if 18","url_template":"https://jaunithuw.com/?h=13451ad5d7bd5e0551226bbd1eaf962d8ca12d3d&pci={external_id}&ppi={zone}"}'
fixture_data['Offer:[2]'] = '{"name":"Video Player - Video blocked","url_template":"https://jaunithuw.com/?h=befc5c3695c9aaa75255f9b467f2c4a4889c5332&pci={external_id}&ppi={zone}"}'


# Conversions
fixture_data['Conversions:_counter'] = '2'
fixture_data['Conversions:[0]'] = '{"time":{"secs_since_epoch":1550513804,"nanos_since_epoch":380712442},"external_id":"c3vx","status":"lead","revenue":{"value":6000,"currency":"USD"}}'
fixture_data['Conversions:[1]'] = '{"time":{"secs_since_epoch":1550522181,"nanos_since_epoch":838230915},"external_id":"crub","status":"lead","revenue":{"value":6000,"currency":"USD"}}'

# Hits
fixture_data['Hits:_counter'] = '1'
fixture_data['Hits:[0]'] = '{"time":{"secs_since_epoch":1550531809,"nanos_since_epoch":999425442},"campaign_id":"Campaign:[0]","destination_id":"Offer:[0]","click_id":"121501819418456064","cost":{"value":86,"currency":"USD"},"dimensions":{"referer":"http://constintptr.com/afu.php?zoneid=1407888&var=1675303","language":"el-GR,el;q=0.9","langcode":"el-GR","useragent":"Mozilla/5.0 (Linux; Android 8.0.0; LDN-L21) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.105 Mobile Safari/537.36","zone":"1675303","connection_type":"BROADBAND","os_version":"8.0.0","ua_vendor":"Google","os":"Android","creative_id":"","ua_category":"smartphone","ua_version":"72.0.3626.105","ua_type":"browser","ua_name":"Chrome","keywords":"","ip":"127.0.0.1","external_id":"c08x"}}'
