from snovault.json_renderer import json_renderer
from snovault.util import get_root_request
from dcicutils.es_utils import create_es_client
from elasticsearch.connection import RequestsHttpConnection
from elasticsearch.serializer import SerializationError
from pyramid.settings import (
    asbool,
    aslist,
)
from .interfaces import (
    APP_FACTORY,
    ELASTIC_SEARCH
)
import json
import sys
from aws_requests_auth.boto_utils import BotoAWSRequestsAuth

PY2 = sys.version_info.major == 2


def includeme(config):
    settings = config.registry.settings

    config.add_request_method(datastore, 'datastore', reify=True)

    address = settings['elasticsearch.server']
    use_aws_auth = settings.get('elasticsearch.aws_auth')
    # make sure use_aws_auth is bool
    if not isinstance(use_aws_auth, bool):
        use_aws_auth = True if use_aws_auth == 'true' else False
    # snovault specific ES options
    # this previously-used option was causing problems (?)
    # 'connection_class': TimedUrllib3HttpConnection
    es_options = {'serializer': PyramidJSONSerializer(json_renderer),
                  'connection_class': TimedRequestsHttpConnection}

    config.registry[ELASTIC_SEARCH] = create_es_client(address,
                                                       use_aws_auth=use_aws_auth,
                                                       **es_options)

    config.include('.cached_views')
    config.include('.esstorage')
    config.include('.indexer_queue')
    config.include('.indexer')
    if asbool(settings.get('mpindexer')) and not PY2:
        config.include('.mpindexer')



def datastore(request):
    if request.__parent__ is not None:
        return request.__parent__.datastore
    datastore = 'database'
    if request.method in ('HEAD', 'GET'):
        datastore = request.params.get('datastore') or \
            request.headers.get('X-Datastore') or \
            request.registry.settings.get('collection_datastore', 'elasticsearch')
    return datastore


class PyramidJSONSerializer(object):
    mimetype = 'application/json'

    def __init__(self, renderer):
        self.renderer = renderer

    def loads(self, s):
        try:
            return json.loads(s)
        except (ValueError, TypeError) as e:
            raise SerializationError(s, e)

    def dumps(self, data):
        # don't serialize strings
        if isinstance(data, (type(''), type(u''))):
            return data

        try:
            return self.renderer.dumps(data)
        except (ValueError, TypeError) as e:
            raise SerializationError(data, e)


# changed to work with Urllib3HttpConnection (from ES) to RequestsHttpConnection
class TimedRequestsHttpConnection(RequestsHttpConnection):
    stats_count_key = 'es_count'
    stats_time_key = 'es_time'

    def stats_record(self, duration):
        request = get_root_request()
        if request is None:
            return

        duration = int(duration * 1e6)
        stats = request._stats
        stats[self.stats_count_key] = stats.get(self.stats_count_key, 0) + 1
        stats[self.stats_time_key] = stats.get(self.stats_time_key, 0) + duration

    def log_request_success(self, method, full_url, path, body, status_code, response, duration):
        self.stats_record(duration)
        return super(RequestsHttpConnection, self).log_request_success(
            method, full_url, path, body, status_code, response, duration)

    def log_request_fail(self, method, full_url, path, body, duration, status_code=None, exception=None):
        self.stats_record(duration)
        return super(RequestsHttpConnection, self).log_request_fail(
            method, full_url, path, body, duration, status_code, exception)
