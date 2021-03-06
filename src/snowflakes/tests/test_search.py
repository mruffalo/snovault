# Use workbook fixture from BDD tests (including elasticsearch)
# these take far to long to run on travis... I'll turn them off there
import pytest
import os
on_travis = os.environ.get('TRAVIS', False) != False
dont_run_on_travis = pytest.mark.skipif(on_travis, reason='to slow to run on travis')
if not on_travis:
    from .features.conftest import app_settings, app, workbook
else:
    app_settings = app = workbook = None

@dont_run_on_travis
def test_search_view(workbook, testapp):
    res = testapp.get('/search/?type=Item').json
    assert res['@type'] == ['Search']
    assert res['@id'] == '/search/?type=Item'
    assert res['@context'] == '/terms/'
    assert res['notification'] == 'Success'
    assert res['title'] == 'Search'
    assert res['total'] > 0
    assert 'facets' in res
    assert 'filters' in res
    assert 'columns' in res
    assert '@graph' in res


@dont_run_on_travis
def test_selective_embedding(workbook, testapp):
    res = testapp.get('/search/?type=Snowflake&limit=all').json
    # Use a specific snowflake, found by accession from test data
    # Check the embedding /types/snow.py entry for Snowflakes; test ensures
    # that the actual embedding matches that
    # the following line fails cause we don't support rev_linked items
    #test_json = [flake for flake in res['@graph'] if flake['accession'] == 'SNOFL001RIC']
    test_json = [flake for flake in res['@graph'] if flake['accession'] == 'SNOFL001MXD']
    if len(test_json) < 1:
        # sometimes the query doesn't work... don't know why...
        res = testapp.get('/search/?type=Snowflake&limit=all').json
        test_json = [flake for flake in res['@graph'] if flake['accession'] == 'SNOFL001MXD']

    assert test_json[0]['lab']['uuid'] == 'cfb789b8-46f3-4d59-a2b3-adc39e7df93a'
    # this specific field should be embedded ('lab.awards.project')
    assert test_json[0]['lab']['awards'][0]['project'] == 'ENCODE'
    # this specific field should be embedded ('lab.awards.title')
    assert test_json[0]['lab']['awards'][0]['title'] == 'A DATA COORDINATING CENTER FOR ENCODE'
    # this specific field was not embedded and should not be present
    assert 'name' not in test_json[0]['lab']['awards'][0]
    # the whole award object should be embedded.
    # test type and a couple keys
    assert isinstance(test_json[0]['award'], dict)
    # default embeds
    assert 'uuid' in test_json[0]['award']
    assert '@id' in test_json[0]['award']
    assert '@type' in test_json[0]['award']
    assert 'principals_allowed' in test_json[0]['award']
    assert 'display_title' in test_json[0]['award']
    # since award.pi was not specifically embedded, pi field should not exist
    # (removed @id-like field)
    assert 'pi' not in test_json[0]['award']
    # @id-like field that should still be embedded (not a valid @id)
    assert test_json[0]['lab']['city'] == 'Stanford/USA/'


@dont_run_on_travis
def recursively_find_uuids(json, uuids):
    for key, val in json.items():
        if key == 'uuid':
            uuids.add(val)
        elif isinstance(val, list):
            for item in val:
                if isinstance(item, dict):
                    uuids = recursively_find_uuids(item, uuids)
        elif isinstance(val, dict):
            uuids = recursively_find_uuids(val, uuids)
    return uuids


@dont_run_on_travis
@pytest.mark.es
def test_linked_uuids_real(workbook, testapp, app):
    """
    Find all uuids from a search result and ensure they match the
    linked_uuids of the es result
    """
    from snovault.elasticsearch.interfaces import ELASTIC_SEARCH
    es = app.registry[ELASTIC_SEARCH]
    res = testapp.get('/search/?type=Snowflake&limit=all').json
    test_case = res['@graph'][0]
    test_uuids = recursively_find_uuids(test_case, set())
    test_doc = es.get(index='snowflake', doc_type='snowflake', id=test_case['uuid'])
    linked_uuids = [link['uuid'] for link in test_doc['_source']['linked_uuids_embedded']]
    # uuids in the doc are a subset of total linked_uuids, which include
    # uuids embedded and referenced in embedded calc properties
    assert set(test_uuids) <= set(linked_uuids)
