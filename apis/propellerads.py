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

    class GroupBy(object):
        CAMPAIGN_ID = 'campaign_id'
        ZONE_ID = 'zone_id'
        GEO = 'geo'
        OS = 'os'
        OS_TYPE = 'os_type'
        OS_VERSION = 'os_version'
        DATE = 'date'
        DEVICE = 'device'
        DEVICE_TYPE = 'device_type'
        BROWSER = 'browser'
        LANG = 'lang'
        ISP = 'isp'
        MOBILE_ISP = 'mobile_isp'

        ALL = [CAMPAIGN_ID, ZONE_ID, GEO, OS, OS_TYPE, OS_VERSION, DATE, DEVICE, DEVICE_TYPE, BROWSER, LANG, ISP, MOBILE_ISP]

        @classmethod
        def is_valid_grouping(cls, *groupbys):
            return all([x in cls.ALL for x in groupbys])


    def __init__(self, username, password, logger=logging.getLogger('propellerads')):
        self.username = username
        self.password = password
        self._token = None
        self._token_will_expire = None
        self.log = logger

        self.log.info("Api Initialized")

    def authorize(self, urlpath='/adv/login'):
        if self.is_authorized():
            self.log.debug("The user %s is already authorized! Token will expire on %s", self.username, self.when_authorization_will_expire())
            return

        self.log.info("Authorization with credentials: %s / %s", self.username, '*' * len(self.password))

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

    def _list_error_or_result(self, response):
        response_obj = self._error_or_result(response)

        items = response_obj['result']
        total_items = int(response_obj['meta']['total_items'])
        total_pages = int(response_obj['meta']['total_pages'])
        page_size = int(response_obj['meta']['page_size'])
        page = int(response_obj['meta']['page'])

        return (items, page, total_pages)

    def _query_multifield(self, field_name, *values):
        return ('&'.join(["%s[]=%s" % (field_name, str(x)) for x in values])) or ''

    '''
    Function for iterating over result that contains multiple items
    '''
    def list_query(self, name, urlpath, headers, querystring="", per_page=50):
        self.log.info("Requesting list of %s", name)

        # Extend provided querystring with page_size and page fields
        query = "%s%s%s" % (querystring, ("" if querystring == "" else "&"), "page_size=%s" % per_page)

        page = 0
        total_pages = 1
        while page < total_pages:
            page = page + 1

            target_url = "%s%s?%s&page=%s" % (self.REST_URL, urlpath, query, page)
            self.log.debug("Request: %s", target_url)

            items, page, total_pages = self._list_error_or_result(requests.request("GET", target_url, headers=headers))

            self.log.info("..page #%s", page)

            for item in items:
                yield item


    def campaigns_all(self, name="campaigns", urlpath='/adv/campaigns', querystring="", per_page=50):
        return self.list_query(name=name,
                               urlpath=urlpath,
                               headers=self._auth_headers(),
                               querystring=querystring,
                               per_page=per_page)

    def campaigns_by_statuses(self, *statuses, **kwargs):
        if not self.Status.is_valid_status(*statuses):
            self.log.critical("Invalid statuses provided %s", statuses)
            raise Exception('Invalid status provided')

        return self.campaigns_all(name="campaigns with statuses: %s" % statuses,
                                  querystring=self._query_multifield('status', *statuses))


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

    def get_statistics(self, date_from, date_to, campaign_ids=(), zone_ids=(), group_by=(), urlpath='/adv/statistics', per_page=50):
        if not self.GroupBy.is_valid_grouping(*group_by):
            self.log.critical("Invalid 'group by' provided %s", group_by)
            raise Exception("Invalid 'group by' provided! %s", group_by)

        querystring = "&".join((self._query_multifield('campaign_id', *campaign_ids),
                            self._query_multifield('zone_id', *zone_ids),
                            self._query_multifield('group_by', *group_by),
                            "date_from=%s" % date_from.strftime('%Y-%m-%d'),
                            "date_to=%s" % date_to.strftime('%Y-%m-%d')))

        self.log.debug("URL: %s", urlpath)
        self.log.debug("Querystring: %s", querystring)

        return self.list_query(name="table values of statistics",
                               urlpath=urlpath,
                               headers=self._auth_headers(),
                               querystring=querystring,
                               per_page=per_page)
