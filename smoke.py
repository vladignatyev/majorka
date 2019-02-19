import logging
from datetime import timedelta, datetime
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

        ALL = [DRAFT, MODERATION, REJECTED, READY, TEST_RUN, WORKING, PAUSED, STOPPED, COMPLETED]

        @classmethod
        def is_valid_status(cls, *statuses):
            return all([x in cls.ALL for x in statuses])

    def __init__(self, username, password, logger=logging.getLogger('propellerads')):
        self.username = username
        self.password = password
        self._token = None
        self._token_will_expire = None
        self.log = logger

        self.log.info("Api Initialized")

    def authorize(self, urlpath='/adv/login'):
        self.log.info("Authorization with credentials: %s / %s", self.username, '*' * len(self.password))

        if self.is_authorized():
            self.log.debug("The user %s is already authorized! Token will expire on %s", self.username, self.when_authorization_will_expire())
            return

        target_url = "%s%s" % (self.REST_URL, urlpath)
        payload = json.dumps({'username': self.username, 'password': self.password})
        headers = {
            'Content-Type': "application/json",
            'Cache-Control': "no-cache"
        }

        json_response = self._error_or_result(requests.request("POST", target_url, data=payload, headers=headers))
        self._token = json_response['api_token']
        self._token_will_expire = datetime.now() + timedelta(seconds=int(json_response['expires_in']))
        self.log.info("New authorization token acquired. Will expire on %s", self.when_authorization_will_expire())

    def authorized(self):
        self.authorize()
        return self

    def is_authorized(self):
        if self._token is None or self._token_will_expire is None:
            return False
        if datetime.now() > self._token_will_expire:
            return False
        return True

    def when_authorization_will_expire(self):
        return self._token_will_expire

    def _auth_headers(self):
        return { 'Authorization': 'Bearer %s' % self._token }
    def _content_type(self):
        return { 'Content-Type': 'application/json' }
    def _json_headers(self):
        return dict(self._auth_headers(), **self._content_type())

    def _error_or_result(self, response):
        response_obj = json.loads(response.text)
        self.log.debug("Received: %s", response.text)
        self.log.info("Status %s", response.status_code)

        if "error" in response_obj.keys() or "errors" in response_obj.keys():
            self.log.critical("Request to API returned an error: %s %s", response_obj.get('message', 'Error'), response_obj['errors'], extra={'response': response_obj})
            raise Exception("'%s': %s" % (response_obj.get('message', 'Error'), response_obj['errors']))

        return response_obj

    # todo: add support of multiple pages of response
    # !!! DRAFT !!!
    def campaigns_all(self, urlpath='/adv/campaigns'):
        self.log.info("Requesting info about campaigns")
        target_url = "%s%s" % (self.REST_URL, urlpath)
        return self._error_or_result(requests.request("GET", target_url, headers=self._auth_headers()))
    #
    # # !!! DRAFT !!!
    def campaigns_by_statuses(self, *statuses):
        self.log.info("Requesting info about campaigns with statuses: %s", statuses)
        if not self.Status.is_valid_status(*statuses):
            self.log.critical("Invalid statuses provided %s", statuses)
            raise Exception('Invalid status provided')
        status_str = '&'.join(["status[]=%s" % str(x) for x in statuses])

        urlpath = '/adv/campaigns?%s' % status_str
        self.log.debug("The destination URL will be: %s", urlpath)
        return self.campaigns_all(urlpath=urlpath)

    def campaign_stop_by_id(self, *campaign_ids, **kwargs):
        if len(campaign_ids) == 0:
            return None

        self.log.info("Stopping campaigns by ID: %s", campaign_ids)

        urlpath = kwargs.get('urlpath', '/adv/campaigns/stop')
        target_url = "%s%s" % (self.REST_URL, urlpath)
        payload = json.dumps({'campaign_ids': campaign_ids})

        return self._error_or_result(requests.request("PUT", target_url, headers=self._json_headers(), data=payload))

    def campaign_start_by_id(self, *campaign_ids, **kwargs):
        self.log.info("Starting campaigns by ID: %s", campaign_ids)
        return self.campaign_stop_by_id(*campaign_ids, urlpath='/adv/campaigns/play')

    def campaign_get_include_zones(self, campaign_id, urlpath='/adv/campaigns/{campaign_id}/targeting/include/zone?campaignId={campaign_id}'):
        self.log.info("Getting 'include' zone targeting for campaign: %s", campaign_id)
        target_url = "%s%s" % (self.REST_URL, urlpath.format(**{'campaign_id': campaign_id}))
        result = self._error_or_result(requests.request("GET", target_url, headers=self._json_headers()))
        return result['zone']

    def campaign_get_exclude_zones(self, campaign_id, urlpath='/adv/campaigns/{campaign_id}/targeting/exclude/zone?campaignId={campaign_id}'):
        self.log.info("Getting 'exclude' zone targeting for campaign: %s", campaign_id)
        return self.campaign_get_include_zones(campaign_id, urlpath=urlpath)

    def campaign_info_by_id(self, campaign_id, urlpath='/adv/campaigns/{campaign_id}'):
        self.log.info("Getting full campaign info for campaign: %s", campaign_id)
        target_url = "%s%s" % (self.REST_URL, urlpath.format(**{'campaign_id': campaign_id}))
        return self._error_or_result(requests.request("GET", target_url, headers=self._json_headers()))



logging.basicConfig(format='[%(name)s] %(asctime)-15s %(message)s')
logger = logging.getLogger('propellerads')
logger.setLevel(logging.INFO)

from credentials import credentials
propeller = PropellerAds(credentials['username'], credentials['password'], logger=logger)
# p.authorize()
# assert p.is_authorized()

propeller.authorized().campaigns_by_statuses(PropellerAds.Status.WORKING)
propeller.authorized().campaigns_by_statuses(PropellerAds.Status.STOPPED)

# print json.dumps(propeller.authorized().campaigns_by_statuses(PropellerAds.Status.WORKING), indent=4, sort_keys=True)

# print json.dumps(propeller.authorized().campaigns_by_statuses(PropellerAds.Status.STOPPED), indent=4, sort_keys=True)
# print propeller.authorized().campaign_start_by_id(1790281)

# propeller.authorized().campaign_stop_by_id(1791408)
# print propeller.authorized().campaign_start_by_id(1790281)

# print propeller.authorized().campaign_get_exclude_zones(1734282)
# assert len(propeller.authorized().campaign_get_exclude_zones(1734282)) == 25

print propeller.authorized().campaign_info_by_id(1734282)

# print propeller.authorized().campaigns_by_statuses(PropellerAds.Status.STOPPED)
