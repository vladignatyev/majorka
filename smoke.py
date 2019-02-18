
import json
import requests



class PropellerAds(object):
    REST_URL = "https://ssp-api.propellerads.com/v5"

    class Status(object):
        DRAFT = 1
        MODERATION = 2
        REJECTED = 3
        READY = 4
        TEST_RUN = 5
        WORKING = 6
        PAUSED = 7
        STOPPED = 8
        COMPLETED = 9

        ANY = [DRAFT, MODERATION, REJECTED, READY, TEST_RUN, WORKING, PAUSED, STOPPED, COMPLETED]

    def __init__(self, username, password):
        self.username = username
        self.password = password
        self._token = None

    def authorize(self, urlpath='/adv/login'):
        target_url = "%s%s" % (self.REST_URL, urlpath)
        payload = json.dumps({'username': self.username, 'password': self.password})
        headers = {
            'Content-Type': "application/json",
            'Cache-Control': "no-cache"
            }

        response = requests.request("POST", target_url, data=payload, headers=headers)
        if response.status_code == 200:
            json_response = json.loads(response.text)
            self._token = json_response['api_token']
        else:
            raise Exception('Invalid Username or Password')

    def is_authorized(self):
        return self._token is not None

    def campaigns_all(self, urlpath='/adv/campaigns'):
        headers = {
            'Authorization': 'Bearer %s' % self._token
        }
        target_url = "%s%s" % (self.REST_URL, urlpath)
        response = requests.request("GET", target_url, headers=headers)
        return json.loads(response.text)

    def campaigns_by_statuses(self, *statuses):
        if not all([x in self.Status.ANY for x in statuses]):
            raise Exception('Invalid status provided')
        #
        status_str = '&'.join(["status[]=%s" % str(x) for x in statuses])

        urlpath = '/adv/campaigns?%s' % status_str
        return self.campaigns_all(urlpath=urlpath)



from credentials import CREDENTIALS;

p = PropellerAds(**CREDENTIALS)
p.authorize()
assert p.is_authorized()

print json.dumps(p.campaigns_by_statuses(PropellerAds.Status.WORKING), indent=4, sort_keys=True)
