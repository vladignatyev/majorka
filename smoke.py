import json
import requests


REST_URL = "https://ssp-api.propellerads.com/v5"

class PropellerAds(object):
    @classmethod
    def authorize(cls, username, password, urlpath='/adv/login'):
        target_url = "%s%s" % (REST_URL, urlpath)
        payload = json.dumps({'username': username, 'password': password})
        headers = {
            'Content-Type': "application/json",
            'Cache-Control': "no-cache"
            }

        response = requests.request("POST", target_url, data=payload, headers=headers)
        return response


print (PropellerAds.authorize(username='ya.na.pochte@gmail.com', password='**password**')).text
