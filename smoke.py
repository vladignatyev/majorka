
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

        json_response = self._error_or_result(requests.request("POST", target_url, data=payload, headers=headers))
        self._token = json_response['api_token']

    def is_authorized(self):
        return self._token is not None

    def _auth_headers(self):
        return { 'Authorization': 'Bearer %s' % self._token }
    def _content_type(self):
        return { 'Content-Type': 'application/json' }

    def _error_or_result(self, response):
        response_obj = json.loads(response.text)

        if "error" in response_obj.keys() or "errors" in response_obj.keys():
            raise Exception("'%s': %s" % (response_obj.get('message', 'Error'), response_obj['errors']))

        return response_obj

    def campaigns_all(self, urlpath='/adv/campaigns'):
        target_url = "%s%s" % (self.REST_URL, urlpath)
        return self._error_or_result(requests.request("GET", target_url, headers=self._auth_headers()))

    def campaigns_by_statuses(self, *statuses):
        if not all([x in self.Status.ANY for x in statuses]):
            raise Exception('Invalid status provided')
        status_str = '&'.join(["status[]=%s" % str(x) for x in statuses])

        urlpath = '/adv/campaigns?%s' % status_str
        return self.campaigns_all(urlpath=urlpath)

    def campaign_stop_by_id(self, *campaign_ids, **kwargs):
        if len(campaign_ids) == 0:
            return None

        urlpath = kwargs.get('urlpath', '/adv/campaigns/stop')
        target_url = "%s%s" % (self.REST_URL, urlpath)
        headers = dict(self._auth_headers(), **self._content_type())
        payload = json.dumps({'campaign_ids': campaign_ids})

        return self._error_or_result(requests.request("PUT", target_url, headers=headers, data=payload))

    def campaign_start_by_id(self, *campaign_ids, **kwargs):
        return self.campaign_stop_by_id(*campaign_ids, urlpath='/adv/campaigns/play')



from credentials import credentials

p = PropellerAds(**credentials)
p.authorize()
assert p.is_authorized()

print json.dumps(p.campaigns_by_statuses(PropellerAds.Status.WORKING), indent=4, sort_keys=True)
# print p.campaign_stop_by_id(1790281)
# print p.campaign_start_by_id(1790281)
