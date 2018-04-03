""" Test full indexing setup

The fixtures in this module setup a full system with postgresql and
elasticsearch running as subprocesses.
Does not include data dependent tests
"""

import pytest
import time
from snovault.elasticsearch.interfaces import ELASTIC_SEARCH, INDEXER_QUEUE, INDEXER_QUEUE_MIRROR
from snovault import (
    COLLECTIONS,
    TYPES,
)

pytestmark = [pytest.mark.indexing]
pytest_plugins = [
    'snovault.tests.indexfixtures',
]


def test_indexing_simple(app, testapp, indexer_testapp):
    from snovault.elasticsearch import indexer_utils
    # First post a single item so that subsequent indexing is incremental
    testapp.post_json('/testing-post-put-patch/', {'required': ''})
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    assert res.json['indexing_status'] == 'finished'
    assert res.json['errors'] is None
    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    uuid = res.json['@graph'][0]['uuid']
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    res = testapp.get('/search/?type=TestingPostPutPatch')
    uuids = [indv_res['uuid'] for indv_res in res.json['@graph']]
    count = 0
    while uuid not in uuids and count < 20:
        time.sleep(1)
        res = testapp.get('/search/?type=TestingPostPutPatch')
        uuids = [indv_res['uuid'] for indv_res in res.json['@graph']]
        count += 1
    assert res.json['total'] >= 2
    assert uuid in uuids
    # test the meta index
    es = app.registry[ELASTIC_SEARCH]
    indexing_doc = es.get(index='meta', doc_type='meta', id='latest_indexing')
    indexing_source = indexing_doc['_source']
    utils_xmin = indexer_utils.get_xmin_from_es(es)
    assert 'indexing_finished' in indexing_source
    assert 'indexing_content' in indexing_source
    assert indexing_source['indexing_status'] == 'finished'
    assert indexing_source['indexing_count'] > 0
    testing_ppp_meta = es.get(index='meta', doc_type='meta', id='testing_post_put_patch')
    testing_ppp_source = testing_ppp_meta['_source']
    assert 'mappings' in testing_ppp_source
    assert 'settings' in testing_ppp_source


def test_indexing_queue_records(app, testapp, indexer_testapp):
    """
    Do a full test using different forms of create mapping and both sync
    and queued indexing.
    """
    from snovault.elasticsearch import create_mapping, indexer_utils
    from elasticsearch.exceptions import NotFoundError
    from datetime import datetime
    es = app.registry[ELASTIC_SEARCH]
    indexer_queue = app.registry[INDEXER_QUEUE]
    test_type = 'testing_post_put_patch'
    # no documents added yet
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    assert doc_count == 0
    # post a document but do not yet index
    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    assert doc_count == 0
    # indexing record should not yet exist (expect error)
    with pytest.raises(NotFoundError):
        es.get(index='meta', doc_type='meta', id='latest_indexing')
    res = indexer_testapp.post_json('/index', {'record': True})
    assert res.json['indexing_count'] == 1
    assert res.json['indexing_content']['type'] == 'queue'
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    assert doc_count == 1
    # make sure latest_indexing doc matches
    indexing_doc = es.get(index='meta', doc_type='meta', id='latest_indexing')
    indexing_doc_source = indexing_doc.get('_source', {})
    assert indexing_doc_source.get('indexing_count') == 1
    # test timing in indexing doc
    assert indexing_doc_source.get('indexing_elapsed')
    indexing_start = indexing_doc_source.get('indexing_started')
    indexing_end = indexing_doc_source.get('indexing_finished')
    assert indexing_start and indexing_end
    time_start =  datetime.strptime(indexing_start, '%Y-%m-%dT%H:%M:%S.%f')
    time_done = datetime.strptime(indexing_end, '%Y-%m-%dT%H:%M:%S.%f')
    assert time_start < time_done
    # get indexing record by start_time
    indexing_record = es.get(index='meta', doc_type='meta', id=indexing_start)
    assert indexing_record.get('_source', {}).get('indexing_status') == 'finished'
    assert indexing_record.get('_source') == indexing_doc_source


def test_sync_and_queue_indexing(app, testapp, indexer_testapp):
    from snovault.elasticsearch import create_mapping, indexer_utils
    from elasticsearch.exceptions import NotFoundError
    from datetime import datetime
    es = app.registry[ELASTIC_SEARCH]
    indexer_queue = app.registry[INDEXER_QUEUE]
    test_type = 'testing_post_put_patch'
    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    # synchronously index
    create_mapping.run(app, collections=[test_type], sync_index=True)
    time.sleep(6)
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    assert doc_count == 1
    assert indexer_queue.number_of_messages()['waiting'] == 0
    indexing_doc = es.get(index='meta', doc_type='meta', id='latest_indexing')
    assert indexing_doc['_source']['indexing_content']['type'] == 'sync'
    assert indexing_doc['_source']['indexing_count'] == 1
    # post second item to database but do not index (don't load into es)
    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    time.sleep(2)
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    # doc_count has not yet updated
    assert doc_count == 1
    # run create mapping to queue the all items
    create_mapping.run(app, collections=[test_type])
    assert indexer_queue.number_of_messages()['waiting'] == 2
    res = indexer_testapp.post_json('/index', {'record': True})
    assert indexing_doc_source.get('indexing_count') == 2
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    assert doc_count == 2


def test_indexer_queue(app):
    indexer_queue_mirror = app.registry[INDEXER_QUEUE_MIRROR]
    # this is only set up for webprod/webprod2
    assert indexer_queue_mirror is None

    indexer_queue = app.registry[INDEXER_QUEUE]
    # unittesting the QueueManager
    assert indexer_queue.queue_url is not None
    assert indexer_queue.dlq_url is not None
    # first, purge the queue
    indexer_queue.clear_queue()
    received = indexer_queue.receive_messages()
    assert len(received) == 0
    test_message = 'abc123'
    to_index, failed = indexer_queue.add_uuids(app.registry, [test_message], strict=True)
    assert to_index == [test_message]
    assert not failed
    received = indexer_queue.receive_messages()
    assert len(received) == 1
    assert received[0]['Body'] == test_message
    # try to receive again (should be empty)
    received_2 = indexer_queue.receive_messages()
    assert len(received_2) == 0
    # replace into queue
    indexer_queue.replace_messages(received)
    received = indexer_queue.receive_messages()
    assert len(received) == 1
    # finally, delete
    indexer_queue.delete_messages(received)
    time.sleep(2)
    msg_count = indexer_queue.number_of_messages()
    assert msg_count['waiting'] == 0
    assert msg_count['inflight'] == 0


def test_es_indices(app, elasticsearch):
    """
    Test overall create_mapping functionality using app.
    Do this by checking es directly before and after running mapping.
    Delete an index directly, run again to see if it recovers.
    """
    from snovault.elasticsearch.create_mapping import type_mapping, create_mapping_by_type, build_index_record
    es = app.registry[ELASTIC_SEARCH]
    item_types = app.registry[TYPES].by_item_type
    # check that mappings and settings are in index
    for item_type in item_types:
        item_mapping = type_mapping(app.registry[TYPES], item_type)
        try:
            item_index = es.indices.get(index=item_type)
        except:
            assert False
        found_index_mapping = item_index.get(item_type, {}).get('mappings').get(item_type, {}).get('properties', {}).get('embedded')
        found_index_settings = item_index.get(item_type, {}).get('settings')
        assert found_index_mapping
        assert found_index_settings
        # get the item record from meta and compare that
        full_mapping = create_mapping_by_type(item_type, app.registry)
        item_record = build_index_record(full_mapping, item_type)
        try:
            item_meta = es.get(index='meta', doc_type='meta', id=item_type)
        except:
            assert False
        meta_record = item_meta.get('_source', None)
        assert meta_record
        assert item_record == meta_record


def test_index_settings(app, testapp, indexer_testapp):
    from snovault.elasticsearch.create_mapping import index_settings
    test_type = 'testing_post_put_patch'
    es_settings = index_settings(test_type)
    max_result_window = es_settings['index']['max_result_window']
    # preform some initial indexing to build meta
    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    res = indexer_testapp.post_json('/index', {'record': True})
    # need to make sure an xmin was generated for the following to work
    assert 'indexing_finished' in res.json
    es = app.registry[ELASTIC_SEARCH]
    curr_settings = es.indices.get_settings(index=test_type)
    found_max_window = curr_settings.get(test_type, {}).get('settings', {}).get('index', {}).get('max_result_window', None)
    # test one important setting
    assert int(found_max_window) == max_result_window


# some unit tests associated with build_index in create_mapping
def test_check_if_index_exists(app):
    from snovault.elasticsearch.create_mapping import check_if_index_exists
    es = app.registry[ELASTIC_SEARCH]
    test_type = 'testing_post_put_patch'
    exists = check_if_index_exists(es, test_type, True)
    assert exists
    # delete index
    es.indices.delete(index=test_type)
    exists = check_if_index_exists(es, test_type, True)
    assert not exists


def test_get_previous_index_record(app):
    from snovault.elasticsearch.create_mapping import get_previous_index_record
    es = app.registry[ELASTIC_SEARCH]
    test_type = 'testing_post_put_patch'
    record = get_previous_index_record(True, es, test_type)
    assert record
    assert 'mappings' in record
    assert 'settings' in record
    # remove index record
    es.delete(index='meta', doc_type='meta', id=test_type)
    record = get_previous_index_record(True, es, test_type)
    assert record is None


def test_check_and_reindex_existing(app, testapp):
    from snovault.elasticsearch.create_mapping import check_and_reindex_existing
    es = app.registry[ELASTIC_SEARCH]
    test_type = 'testing_post_put_patch'
    # post an item but don't reindex
    # this will cause the testing-ppp index to queue reindexing when we call
    # check_and_reindex_existing
    res = testapp.post_json('/testing-post-put-patch/', {'required': ''})
    time.sleep(2)
    doc_count = es.count(index=test_type, doc_type=test_type).get('count')
    # doc_count has not yet updated
    assert doc_count == 0
    test_uuids = set()
    check_and_reindex_existing(app, es, test_type, test_uuids)
    assert(len(test_uuids)) == 1


def test_es_delete_simple(app, testapp, indexer_testapp, session):
    from snovault.interfaces import STORAGE
    from snovault.storage import (
        Resource,
        Key,
        PropertySheet,
        CurrentPropertySheet,
        TransactionRecord,
    )
    from snovault.elasticsearch.create_mapping import (
       run as run_create_mapping,
       check_and_reindex_existing
    )
    from snovault.commands.es_index_data import run as run_index_data
    indexer_queue = app.registry[INDEXER_QUEUE]
    ## Adding new test resource to DB
    storage = app.registry[STORAGE]
    test_type = 'testing_post_put_patch'
    test_body = {'required': '', 'simple1' : 'foo', 'simple2' : 'bar' }
    res = testapp.post_json('/testing-post-put-patch/', test_body)
    test_uuid = res.json['@graph'][0]['uuid']
    check = storage.get_by_uuid(test_uuid)

    assert str(check.uuid) == test_uuid

    ## Make sure we update ES index
    test_uuids = set()
    check_and_reindex_existing(app, app.registry[ELASTIC_SEARCH], test_type, test_uuids)

    assert test_uuid in test_uuids # Assert that this newly added Item is not yet indexed.
    # Then index it:
    indexer_queue.add_uuids(app.registry, list(test_uuids), strict=True)
    res = indexer_testapp.post_json('/index', {'record': True})
    time.sleep(5) # INDEXER performs a network request to ES server to index things. Whether we like it or not, this means it's async and we must wait.

    ## Now ensure that we do have it in ES:
    test_uuids_2 = set()
    check_and_reindex_existing(app, app.registry[ELASTIC_SEARCH], test_type, test_uuids_2)
    assert bool(test_uuids_2) == False # Ensure we don't have any more indices to reindex after indexing our newly added UUID/Item

    check_post_from_es = storage.read.get_by_uuid(test_uuid)
    check_post_from_rdb = storage.write.get_by_uuid(test_uuid)

    assert check_post_from_es is not None
    assert check_post_from_rdb is not None

    assert check_post_from_es.properties['simple1'] == test_body['simple1']
    assert check_post_from_es.properties['simple2'] == test_body['simple2']

    # The actual delete
    storage.delete_by_uuid(test_uuid) # We can optionally pass in test_type as well for better performance.

    check_post_from_rdb_2 = storage.write.get_by_uuid(test_uuid)

    assert check_post_from_rdb_2 is None

    time.sleep(5) # Allow time for ES API to send network request to ES server to perform delete.
    check_post_from_es_2 = storage.read.get_by_uuid(test_uuid)
    assert check_post_from_es_2 is None
