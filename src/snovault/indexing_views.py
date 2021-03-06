from __future__ import unicode_literals
from pyramid.security import (
    Authenticated,
    Everyone,
    principals_allowed_by_permission,
)
from pyramid.traversal import resource_path
from pyramid.view import view_config
from pyramid.settings import asbool
from timeit import default_timer as timer
from .resources import Item
from .authentication import calc_principals
from .interfaces import STORAGE
from .elasticsearch.indexer_utils import find_uuids_for_indexing

def includeme(config):
    config.add_route('indexing-info', '/indexing-info')
    config.scan(__name__)


# really simple exception to know when the sid check fails
class SidException(Exception):
    pass


def join_linked_uuids_sids(request, uuids):
    """
    Simply iterate through the uuids and return an array of dicts containing
    uuid and sid (from request._sid_cache)

    Args:
        request: current Request object
        uuids: list of string uuids

    Returns:
        A list of dicts containing uuid and up-to-date db sid
    """
    return [{'uuid': uuid, 'sid': request._sid_cache[uuid]} for uuid in uuids]


def get_rev_linked_items(request, uuid):
    """
    Iterate through request._rev_linked_uuids_by_item, which is populated
    during the embedding traversal process, to find the items that are reverse
    linked to the given uuid

    Args:
        request: current Request object
        uuid (str): uuid of the object in question

    Returns:
        A set of string uuids
    """
    # find uuids traversed that rev link to this item
    rev_linked_to_me = set()
    for rev_id, rev_names in request._rev_linked_uuids_by_item.items():
        if any([uuid in rev_names[name] for name in rev_names]):
            rev_linked_to_me.add(rev_id)
            continue
    return rev_linked_to_me


@view_config(context=Item, name='index-data', permission='index', request_method='GET')
def item_index_data(context, request):
    """
    Very important view which is used to calculate all the data indexed in ES
    for the given item. If an int sid is provided as a request parameter,
    will raise an sid exception if the current item context is behind the
    given sid.
    Computationally intensive. Calculates the object, embedded, and audit views
    for the given item, using ES results where possible for speed. Leverages
    a number of attributes on the request to get information indexing needs.

    Args:
        context: current Item
        request: current Request

    Returns:
        A dict document representing the full data to index for the given item
    """
    uuid = str(context.uuid)
    # upgrade_properties calls necessary upgraders based on schema_version
    properties = context.upgrade_properties()

    # if we want to check an sid, it should be set as a query param
    sid_check = request.params.get('sid', None)
    if sid_check:
        try:
            sid_check = int(sid_check)
        except ValueError:
            raise ValueError('sid parameter must be an integer. Provided sid: %s' % sid)
        if context.sid < sid_check:
            raise SidException('sid from the query (%s) is greater than that on context (%s). Bailing.' % (sid_check, context.sid))

    # ES versions 2 and up don't allow dots in links. Update these to use ~s
    new_links = {}
    for key, val in context.links(properties).items():
        new_links['~'.join(key.split('.'))] = val
    links = new_links
    unique_keys = context.unique_keys(properties)

    principals_allowed = calc_principals(context)
    path = resource_path(context)
    paths = {path}
    collection = context.collection

    if collection.unique_key in unique_keys:
        paths.update(
            resource_path(collection, key)
            for key in unique_keys[collection.unique_key])

    for base in (collection, request.root):
        for key_name in ('accession', 'alias'):
            if key_name not in unique_keys:
                continue
            paths.add(resource_path(base, uuid))
            paths.update(
                resource_path(base, key)
                for key in unique_keys[key_name])

    path = path + '/'
    # setting _indexing_view enables the embed_cache and cause population of
    # request._linked_uuids and request._rev_linked_uuids_by_item
    request._indexing_view = True

    # run the object view first
    request._linked_uuids = set()
    object_view = request.invoke_view(path, '@@object')
    linked_uuids_object = request._linked_uuids.copy()
    rev_link_names = request._rev_linked_uuids_by_item.get(uuid, {}).copy()

    # reset these properties, then run embedded view
    request._linked_uuids = set()
    request._audit_uuids = set()
    request._rev_linked_uuids_by_item = {}
    request._aggregate_for['uuid'] = uuid
    request._aggregated_items = {
        agg: {'_fields': context.aggregated_items[agg], 'items': []} for agg in context.aggregated_items
    }
    # since request._indexing_view is set to True in indexer.py,
    # all embeds (including subrequests) below will use the embed cache
    embedded_view = request.invoke_view(path, '@@embedded', index_uuid=uuid)
    # get _linked and _rev_linked uuids from the request before @@audit views add to them
    linked_uuids_embedded = request._linked_uuids.copy()

    # find uuids traversed that rev link to this item
    rev_linked_to_me = get_rev_linked_items(request, uuid)
    aggregated_items = {agg: res['items'] for agg, res in request._aggregated_items.items()}

    # lastly, run the audit view. Set the uuids we want to audit on
    request._audit_uuids = list(linked_uuids_embedded)
    audit_view = request.invoke_view(path, '@@audit')['audit']

    document = {
        'aggregated_items': aggregated_items,
        'audit': audit_view,
        'embedded': embedded_view,
        'item_type': context.type_info.item_type,
        'linked_uuids_embedded': join_linked_uuids_sids(request, linked_uuids_embedded),
        'linked_uuids_object': join_linked_uuids_sids(request, linked_uuids_object),
        'links': links,
        'object': object_view,
        'paths': sorted(paths),
        'principals_allowed': principals_allowed,
        'properties': properties,
        'propsheets': {
            name: context.propsheets[name]
            for name in context.propsheets.keys() if name != ''
        },
        'rev_link_names': rev_link_names,
        'sid': context.sid,
        'unique_keys': unique_keys,
        'uuid': uuid,
        'rev_linked_to_me': sorted(rev_linked_to_me)
    }

    return document


@view_config(route_name='indexing-info', permission='index', request_method='GET')
def indexing_info(request):
    """
    Endpoint to check some indexing-related properties of a given uuid, which
    is provided using the `uuid=` query parameter. This route cannot be defined
    with the context of a specific Item because that will cause the underlying
    request to use a cached view from Elasticsearch and not properly run
    the @@embedded view from the database.

    If you do not want to calculate the embedded object, use `run=False`

    Args:
        request: current Request object

    Returns:
        dict response
    """
    uuid = request.params.get('uuid')
    if not uuid:
        return {'status': 'error', 'title': 'Error', 'message': 'ERROR! Provide a uuid to the query.'}

    db_sid = request.registry[STORAGE].write.get_by_uuid(uuid).sid
    es_sid = request.registry[STORAGE].read.get_by_uuid(uuid).sid
    response = {'sid_db': db_sid, 'sid_es': es_sid, 'title': 'Indexing Info for %s' % uuid}
    if asbool(request.params.get('run', True)):
        request._indexing_view = True
        request.datastore = 'database'
        path = '/' + uuid + '/@@embedded'
        start = timer()
        embedded_view = request.invoke_view(path, index_uuid=uuid, as_user='INDEXER')
        end = timer()
        response['embedded_seconds'] = end - start
        es_assc_uuids = find_uuids_for_indexing(request.registry, set([uuid]))
        new_rev_link_uuids = get_rev_linked_items(request, uuid)
        # invalidated: items linking to this in es + newly rev linked items
        response['uuids_invalidated'] = list(es_assc_uuids | new_rev_link_uuids)
        response['description'] = 'Using live results for embedded view of %s. Query with run=False to skip this.' % uuid
    else:
        response['description'] = 'Query with run=True to calculate live information on invalidation and embedding time.'
    response['status'] = 'success'
    return response
