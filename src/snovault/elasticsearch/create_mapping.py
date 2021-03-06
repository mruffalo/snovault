"""\
Example.

To load the initial data:

    %(prog)s production.ini

"""
from pyramid.paster import get_app
from elasticsearch import RequestError
from elasticsearch.helpers import bulk
from elasticsearch.exceptions import (
    ConflictError,
    ConnectionError,
    NotFoundError,
    TransportError,
    RequestError,
    ConnectionTimeout
)
from elasticsearch_dsl import Index, Search
from elasticsearch_dsl.connections import connections
from functools import reduce
from snovault import (
    COLLECTIONS,
    TYPES,
)
from snovault.schema_utils import combine_schemas
from snovault.util import add_default_embeds, find_collection_subtypes
from .interfaces import ELASTIC_SEARCH, INDEXER_QUEUE
from collections import OrderedDict
from itertools import chain
import json
import structlog
import time
import datetime
import sys
from snovault.commands.es_index_data import run as run_index_data
from .indexer_utils import find_uuids_for_indexing, get_uuids_for_types
import transaction
import os
import argparse
from snovault import set_logging
import logging
from timeit import default_timer as timer


EPILOG = __doc__


log = structlog.getLogger(__name__)

# An index to store non-content metadata
META_MAPPING = {
    '_all': {
        'enabled': False,
    },
    'enabled': False,
}

PATH_FIELDS = ['submitted_file_name']
NON_SUBSTRING_FIELDS = ['uuid', '@id', 'submitted_by', 'md5sum', 'references', 'submitted_file_name']
NUM_SHARDS = 1
NUM_REPLICAS = 1


def sorted_pairs_hook(pairs):
    return OrderedDict(sorted(pairs))


def sorted_dict(d):
    return json.loads(json.dumps(d), object_pairs_hook=sorted_pairs_hook)


def determine_if_is_date_field(field, schema):
    is_date_field = False
    if schema.get('format') is not None:
        if schema['format'] == 'date' or schema['format'] == 'date-time':
            is_date_field = True
    elif schema.get('anyOf') is not None and len(schema['anyOf']) > 1:
        is_date_field = True # Will revert to false unless all anyOfs are format date/datetime.
        for schema_option in schema['anyOf']:
            if schema_option.get('format') not in ['date', 'date-time']:
                is_date_field = False
                break
    return is_date_field


def schema_mapping(field, schema, top_level=False):
    """
    Create the mapping for a given schema. Can handle using all fields for
    objects (*), but can handle specific fields using the field parameter.
    This allows for the mapping to match the selective embedding.
    """
    if 'linkFrom' in schema:
        type_ = 'string'
    else:
        type_ = schema['type']

    # Elasticsearch handles multiple values for a field
    if type_ == 'array' and schema['items']:
        return schema_mapping(field, schema['items'])

    if type_ == 'object':
        properties = {}
        for k, v in schema.get('properties', {}).items():
            mapping = schema_mapping(k, v)
            if mapping is not None:
                if field == '*' or k == field:
                    properties[k] = mapping
        if top_level:
            # only include include_in_all: True in top level
            return {
                'include_in_all': True,
                'properties': properties,
            }
        else:
            return {
                'properties': properties,
            }

    if determine_if_is_date_field(field, schema):
        return {
            'type': 'date',
            'format': "date_optional_time",
            'fields': {
                'raw': {
                    'type': 'keyword'
                },
                'lower_case_sort': {
                    'type': 'text',
                    'analyzer': 'case_insensistive_sort',
                    'fields': {
                        'keyword': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }

    if type_ == ["number", "string"]:
        return {
            'type': 'text',
            'fields': {
                'value': {
                    'type': 'float',
                    'ignore_malformed': True,
                },
                'raw': {
                    'type': 'keyword'
                },
                'lower_case_sort': {
                    'type': 'text',
                    'analyzer': 'case_insensistive_sort',
                    'fields': {
                        'keyword': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }

    if type_ == 'boolean':
        return {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword'
                },
                'lower_case_sort': {
                    'type': 'text',
                    'analyzer': 'case_insensistive_sort',
                    'fields': {
                        'keyword': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }

    if type_ == 'string':
        # don't make a mapping for non-embedded objects
        if 'linkTo' in schema or 'linkFrom' in schema:
            return

        sub_mapping = {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword'
                },
                'lower_case_sort': {
                    'type': 'text',
                    'analyzer': 'case_insensistive_sort',
                    'fields': {
                        'keyword': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }

        if field in NON_SUBSTRING_FIELDS:
            if field in PATH_FIELDS:
                sub_mapping['analyzer'] = 'snovault_path_analyzer'
        return sub_mapping

    if type_ == 'number':
        return {
            'type': 'float',
            'fields': {
                'raw': {
                    'type': 'keyword'
                },
                'lower_case_sort': {
                    'type': 'text',
                    'analyzer': 'case_insensistive_sort',
                    'fields': {
                        'keyword': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }

    if type_ == 'integer':
        return {
            'type': 'long',
            'fields': {
                'raw': {
                    'type': 'keyword'
                },
                'lower_case_sort': {
                    'type': 'text',
                    'analyzer': 'case_insensistive_sort',
                    'fields': {
                        'keyword': {
                            'type': 'keyword'
                        }
                    }
                }
            }
        }


def index_settings():
    return {
        'index': {
            'number_of_shards': NUM_SHARDS,
            'number_of_replicas': NUM_REPLICAS,
            'max_result_window': 100000,
            'mapping': {
                'total_fields': {
                    'limit': 5000
                },
                'depth': {
                    'limit': 30
                }
            },
            'analysis': {
                'filter': {
                    'substring': {
                        'type': 'nGram',
                        'min_gram': 1,
                        'max_gram': 33
                    }
                },
                'analyzer': {
                    'default': {
                        'type': 'custom',
                        'tokenizer': 'whitespace',
                        'char_filter': 'html_strip',
                        'filter': [
                            'standard',
                            'lowercase',
                        ]
                    },
                    'snovault_index_analyzer': {
                        'type': 'custom',
                        'tokenizer': 'whitespace',
                        'char_filter': 'html_strip',
                        'filter': [
                            'standard',
                            'lowercase',
                            'asciifolding',
                            'substring'
                        ]
                    },
                    'snovault_search_analyzer': {
                        'type': 'custom',
                        'tokenizer': 'whitespace',
                        'filter': [
                            'standard',
                            'lowercase',
                            'asciifolding'
                        ]
                    },
                    'case_insensistive_sort': {
                        'tokenizer': 'keyword',
                        'filter': [
                            'lowercase',
                        ]
                    },
                    'snovault_path_analyzer': {
                        'type': 'custom',
                        'tokenizer': 'snovault_path_tokenizer',
                        'filter': ['lowercase']
                    }
                },
                'tokenizer': {
                    'snovault_path_tokenizer': {
                        'type': 'path_hierarchy',
                        'reverse': True
                    }
                }
            }
        }
    }


def audit_mapping():
    return {
        'category': {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword'
                }
            }
        },
        'detail': {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword'
                }
            }
        },
        'level_name': {
            'type': 'text',
            'fields': {
                'raw': {
                    'type': 'keyword'
                }
            }
        },
        'level': {
            'type': 'integer',
        }
    }


# generate an index record, which contains a mapping and settings
def build_index_record(mapping, in_type):
    return {
        'mappings': {in_type: mapping},
        'settings': index_settings()
    }


def es_mapping(mapping, agg_items_mapping):
    return {
        '_all': {
            'enabled': True,
            'analyzer': 'snovault_index_analyzer',
            'search_analyzer': 'snovault_search_analyzer'
        },
        'dynamic_templates': [
            {
                'template_principals_allowed': {
                    'path_match': "principals_allowed.*",
                    'mapping': {
                        'index': True,
                        'type': 'keyword',
                    },
                },
            },
            {
                'template_unique_keys': {
                    'path_match': "unique_keys.*",
                    'mapping': {
                        'index': True,
                        'type': 'keyword',
                    },
                },
            },
            {
                'template_links': {
                    'path_match': "links.*",
                    'mapping': {
                        'index': True,
                        'type': 'keyword',
                    },
                },
            },
        ],
        'properties': {
            'uuid': {
                'type': 'text',
                'include_in_all': False,
            },
            'sid': {
                'type': 'long',
                'include_in_all': False,
            },
            'item_type': {
                'type': 'text',
            },
            'embedded': mapping,
            'object': {
                'type': 'object',
                'enabled': False,
                'include_in_all': False,
            },
            'properties': {
                'type': 'object',
                'enabled': False,
                'include_in_all': False,
            },
            'propsheets': {
                'type': 'object',
                'enabled': False,
                'include_in_all': False,
            },
            'principals_allowed': {
                'include_in_all': False,
                'properties': {
                    'view': {
                        'type': 'keyword'
                    },
                    'edit': {
                        'type': 'keyword'
                    },
                    'audit': {
                        'type': 'keyword'
                    }
                }
            },
            'aggregated_items': agg_items_mapping,
            'linked_uuids_embedded': {
                'properties': {
                    'uuid': {
                        'type': 'keyword'
                    },
                    'sid': {
                        'type': 'keyword'
                    }
                },
                'include_in_all': False
            },
            'linked_uuids_object': {
                'properties': {
                    'uuid': {
                        'type': 'keyword'
                    },
                    'sid': {
                        'type': 'keyword'
                    }
                },
                'include_in_all': False
            },
            'rev_link_names': {
                'properties': {
                    'name': {
                        'type': 'keyword'
                    },
                    'uuids': {
                        'type': 'keyword'
                    }
                },
                'include_in_all': False
            },
            'rev_linked_to_me': {
                'type': 'text',
                'include_in_all': False
            },
            'unique_keys': {
                'type': 'object',
                'include_in_all': False
            },
            'links': {
                'type': 'object',
                'include_in_all': False
            },
            'paths': {
                'type': 'text',
                'include_in_all': False
            },
            'audit': {
                'include_in_all': False,
                'properties': {
                    'ERROR': {
                        'properties': audit_mapping()
                    },
                    'NOT_COMPLIANT': {
                        'properties': audit_mapping()
                    },
                    'WARNING': {
                        'properties': audit_mapping()
                    },
                    'INTERNAL_ACTION': {
                        'properties': audit_mapping()
                    },
                },
            }
        }
    }


def aggregated_items_mapping(types, item_type):
    """
    Create the mapping for the aggregated items of the given type.
    This is a simple mapping, since all values can be set as keywords
    (only used for exact match search and not sorted).
    Since the fields for each aggregated item are split by dots, we organize
    these as the hierarchical objects for Elasticsearch
    Args:
        types: result of request.registry[TYPES]
        item_type: string item type that we are creating the mapping for
    Returns:
        Dictionary mapping for the aggrated_items of the given item type
    """
    type_info = types[item_type]
    aggregated_items = type_info.aggregated_items
    mapping = {'include_in_all': False, 'type': 'object'}
    if not aggregated_items:
        return mapping
    del mapping['type']
    mapping['properties'] = aggs_mapping = {}
    for agg_item, agg_fields in aggregated_items.items():
        # include raw field name by convention, though both are keywords
        aggs_mapping[agg_item] = {
            'properties': {
                'parent': {
                    'type': 'text',
                    'fields': {
                        'raw': {
                            'type': 'keyword'
                        }
                    }
                },
                'item': {
                    'properties': {}
                }
            }
        }
        # if no agg fields are provided, default to uuid
        if agg_fields == []:
            agg_fields = ['uuid']
        aggs_mapping[agg_item]['properties']['item']['properties'] = agg_fields_mapping = {}
        for agg_field in agg_fields:
            # elasticsearch models fields with dots as hierarchical objects
            # must compose our mapping like that
            split_field = agg_field.split('.')
            ptr = agg_fields_mapping
            for idx, split_part in enumerate(split_field):
                if idx == len(split_field) - 1:
                    mapping_val = {
                        'type': 'keyword',
                        'fields': {
                            'raw': {
                                'type': 'keyword'
                            }
                        }
                    }
                else:
                    mapping_val = {'properties': {}}
                if (split_part not in ptr or
                    ('properties' in mapping_val and 'properties' not in ptr[split_part])):
                    ptr[split_part] = mapping_val
                if 'properties' in ptr[split_part]:
                    ptr = ptr[split_part]['properties']
                else:
                    break
    return mapping


def type_mapping(types, item_type, embed=True):
    """
    Create mapping for each type. This is relatively simple if embed=False.
    When embed=True, the embedded fields (defined in /types/ directory) will
    be used to generate custom embedding of objects. Embedding paths are
    separated by dots. If the last field is an object, all fields in that
    object will be embedded (e.g. biosource.individual). To embed a specific
    field only, do add it at the end of the path: biosource.individual.title

    No field checking has been added yet (TODO?), so make sure fields are
    spelled correctly.

    Any fields that are not objects will NOT be embedded UNLESS they are in the
    embedded list, again defined in the types .py file for the object.
    """
    type_info = types[item_type]
    schema = type_info.schema
    # use top_level parameter here for schema_mapping
    mapping = schema_mapping('*', schema, True)
    embeds = add_default_embeds(item_type, types, type_info.embedded_list, schema)
    embeds.sort()
    if not embed:
        return mapping
    for prop in embeds:
        single_embed = {}
        curr_s = schema
        curr_m = mapping
        split_embed_path = prop.split('.')
        for curr_e in split_embed_path:
            # if we want to map all fields (*), do not drill into schema
            if curr_e != '*':
                # drill into the schemas. if no the embed is not found, break
                subschema = curr_s.get('properties', {}).get(curr_e, None)
                curr_s = merge_schemas(subschema, types)
            if not curr_s:
                break
            curr_m = update_mapping_by_embed(curr_m, curr_e, curr_s)
    return mapping


def merge_schemas(subschema, types):
    """
    Merge any linked schemas into the current one. Return None if none present
    """
    if not subschema:
        return None
    # handle arrays by simply jumping into them
    # we don't care that they're flattened during mapping
    ref_types = None
    subschema = subschema.get('items', subschema)
    if 'linkFrom' in subschema:
        _ref_type, _ = subschema['linkFrom'].split('.', 1)
        ref_types = [_ref_type]
    elif 'linkTo' in subschema:
        ref_types = subschema['linkTo']
        if not isinstance(ref_types, list):
            ref_types = [ref_types]
    if ref_types is None:
        curr_s = subschema
    else:
        embedded_types = [types[t].schema for t in ref_types
                          if t in types.all]
        if not embedded_types:
            return None
        curr_s = reduce(combine_schemas, embedded_types)
    return curr_s


def update_mapping_by_embed(curr_m, curr_e, curr_s):
    """
    Update the mapping based on the current mapping (curr_m), the current embed
    element (curr_e), and the processed schemas (curr_s).
    when curr_e = '*', it is a special case where all properties are added
    to the object that was previously mapped.
    """
    # see if there's already a mapping associated with this embed:
    # multiple subobjects may be embedded, so be careful here
    mapped = schema_mapping(curr_e, curr_s)
    if curr_e == '*':
        if 'properties' in mapped:
            curr_m['properties'].update(mapped['properties'])
        else:
            curr_m['properties'] = mapped
    elif curr_e in curr_m['properties'] and 'properties' in curr_m['properties'][curr_e]:
        if 'properties' in mapped:
            curr_m['properties'][curr_e]['properties'].update(mapped['properties'])
        else:
            curr_m['properties'][curr_e] = mapped
        curr_m = curr_m['properties'][curr_e]
    else:
        curr_m['properties'][curr_e] = mapped
        curr_m = curr_m['properties'][curr_e]
    return curr_m


def create_mapping_by_type(in_type, registry):
    """
    Return a full mapping for a given doc_type of in_type
    """
    # build a schema-based hierarchical mapping for embedded view
    collection = registry[COLLECTIONS].by_item_type[in_type]
    embed_mapping = type_mapping(registry[TYPES], collection.type_info.item_type)
    agg_items_mapping = aggregated_items_mapping(registry[TYPES], collection.type_info.item_type)
    # finish up the mapping
    return es_mapping(embed_mapping, agg_items_mapping)


def build_index(app, es, in_type, mapping, uuids_to_index, dry_run, check_first, index_diff=False,
                print_count_only=False, cached_meta=None, meta_bulk_actions=None):
    """
    Creates an es index for the given in_type with the given mapping and
    settings defined by item_settings(). If check_first == True, attempting
    to see if the index exists and is unchanged from the previous mapping.
    If so, do not delete and recreate the index to save on indexing.
    This function will trigger a reindexing of the in_type index if
    the old index is kept but the es doc count differs from the db doc count.
    Will also trigger a re-index for a newly created index if the indexing
    document in meta exists and has an xmin.

    please pass in cached_meta and meta_bulk_actions if you are creating multiple indices
    then update meta index once with `elasticsearch.helpers.bulk(es, meta_bulk_actions)`
    passing in `cached_meta` can also save on calls to check if index exists...
    """
    uuids_to_index[in_type] = set()
    if print_count_only:
        log.info('___PRINTING COUNTS___')
        check_and_reindex_existing(app, es, in_type, uuids_to_index, index_diff, True)
        return

    # combines mapping and settings
    this_index_record = build_index_record(mapping, in_type)

    # determine if index already exists for this type
    # probably don't need to do this as I can do it upstream... but passing in meta makes it
    # just a single if check
    this_index_exists = check_if_index_exists(es, in_type, cached_meta)

    # if the index exists, we might not need to delete it
    # otherwise, run if we are using the check-first or index_diff args
    if check_first or index_diff:
        prev_index_record = get_previous_index_record(this_index_exists, es, in_type)
        if prev_index_record is not None and this_index_record == prev_index_record:
            if in_type != 'meta':
                check_and_reindex_existing(app, es, in_type, uuids_to_index, index_diff)
            log.info('MAPPING: using existing index for collection %s' % (in_type), collection=in_type)
            return

    if dry_run or index_diff:
        return

    # delete the index
    if this_index_exists:
        res = es_safe_execute(es.indices.delete, index=in_type, ignore=[400,404])
        if res:
            log.info('MAPPING: index successfully deleted for %s' % in_type, collection=in_type)
        else:
            log.error('MAPPING: could not delete index for %s' % in_type, collection=in_type)

    # first, create the mapping. adds settings and mappings in the body
    res = es_safe_execute(es.indices.create, index=in_type, body=this_index_record, ignore=[400])
    if res:
        log.info('MAPPING: new index created for %s' % (in_type), collection=in_type)
    else:
        log.error('MAPPING: new index failed for %s' % (in_type), collection=in_type)

    # check to debug create-mapping issues and ensure correct mappings
    confirm_mapping(es, in_type, this_index_record)

    # we need to queue items in the index for indexing
    # if check_first and we've made it here, nothing has been queued yet
    # for this collection
    start = timer()
    coll_uuids = set(get_uuids_for_types(app.registry, types=[in_type]))
    end = timer()
    log.info('Time to get collection uuids: %s' % str(end-start), cat='fetch time',
                duration=str(end-start), collection=in_type)
    uuids_to_index[in_type] = coll_uuids
    log.info('MAPPING: will queue all %s items in the new index %s for reindexing' %
             (len(coll_uuids), in_type), cat='items to queue', count=len(coll_uuids), collection=in_type)

    # put index_record in meta
    if meta_bulk_actions is None:
        meta_bulk_actions = []
        # 1-2s faster to load in bulk if your doing more than one
        start = timer()
        res = es_safe_execute(es.index, index='meta', doc_type='meta', body=this_index_record, id=in_type)
        end = timer()
        log.info("Time to update metadata document: %s" % str(end-start), duration=str(end-start),
                    collection=in_type, cat='update meta')
        if res:
            log.info("MAPPING: index record created for %s" % (in_type), collection=in_type)
        else:
            log.error("MAPPING: index record failed for %s" % (in_type), collection=in_type)
    else:
        # create bulk actions to be submitted after all mappings are created
        bulk_action = {'_op_type': 'index',
                       '_index': 'meta',
                       '_type': 'meta',
                       '_id': in_type,
                       '_source': this_index_record
                      }
        meta_bulk_actions.append(bulk_action)
    return meta_bulk_actions


def check_if_index_exists(es, in_type, cached_indices=None):
    # first attempt to get the index from the cached meta indices
    if isinstance(cached_indices, dict) and cached_indices:
        return in_type in cached_indices
    try:
        this_index_exists = es.indices.exists(index=in_type)
        if this_index_exists:
            return this_index_exists
    except ConnectionTimeout:
        this_index_exists = False
    return this_index_exists


def get_previous_index_record(this_index_exists, es, in_type):
    """
    Decide if we need to drop the index + reindex (no index/no meta record)
    OR
    compare previous mapping and current mapping + settings to see if we need
    to update. if not, use the existing mapping to prevent re-indexing.
    """
    prev_index_hit = {}
    if this_index_exists:
        try:
            # multiple queries to meta... don't want this...
            prev_index_hit = es.get(index='meta', doc_type='meta', id=in_type, ignore=[404])
        except TransportError as excp:
            if excp.info.get('status') == 503:
                es.indices.refresh(index='meta')
                time.sleep(3)
                try:
                    prev_index_hit = es.get(index='meta', doc_type='meta', id=in_type, ignore=[404])
                except:
                    return None
        prev_index_record = prev_index_hit.get('_source')
        return prev_index_record
    else:
        return None


def check_and_reindex_existing(app, es, in_type, uuids_to_index, index_diff=False, print_counts=False):
    """
    lastly, check to make sure the item count for the existing
    index matches the database document count. If not, queue the uuids_to_index
    in the index for reindexing.
    If index_diff, store uuids for reindexing that are in DB but not ES
    """
    db_count, es_count, db_uuids, diff_uuids = get_db_es_counts_and_db_uuids(app, es, in_type, index_diff)
    log.info("DB count is %s and ES count is %s for index: %s" %
                (str(db_count), str(es_count), in_type), collection=in_type,
                 db_count=str(db_count), cat='collection_counts', es_count=str(es_count))
    if print_counts:  # just display things, don't actually queue the uuids
        if index_diff and diff_uuids:
            log.info("The following UUIDs are found in the DB but not the ES index: %s\n%s"
                        % (in_type, diff_uuids), collection=in_type)
        return
    if es_count is None or es_count != db_count:
        if index_diff:
            log.info('MAPPING: queueing %s items found in DB but not ES in the index %s for reindexing'
                        % (str(len(diff_uuids)), in_type), items_queued=str(len(diff_uuids)), collection=in_type)
            uuids_to_index[in_type] = diff_uuids
        else:
            log.info('MAPPING: queueing %s items found in the existing index %s for reindexing'
                        % (str(len(db_uuids)), in_type), items_queued=str(len(db_uuids)), collection=in_type)
            uuids_to_index[in_type] = db_uuids


def get_db_es_counts_and_db_uuids(app, es, in_type, index_diff=False):
    """
    Return the database count and elasticsearch count for a given item type,
    the list of collection uuids from the database, and the list of uuids
    found in the DB but not in the ES store.
    """
    if check_if_index_exists(es, in_type):
        if index_diff:
            search = Search(using=es, index=in_type, doc_type=in_type)
            search_source = search.source([])
            es_uuids = set([h.meta.id for h in search_source.scan()])
            es_count = len(es_uuids)
        else:
            count_res = es.count(index=in_type, doc_type=in_type)
            es_count = count_res.get('count')
            es_uuids = set()
    else:
        es_count = 0
        es_uuids = set()
    db_uuids = set(get_uuids_for_types(app.registry, types=[in_type]))
    db_count = len(db_uuids)
    # find uuids in the DB but not ES (set operations)
    if index_diff:
        diff_uuids = db_uuids - es_uuids
    else:
        diff_uuids = set()
    return db_count, es_count, db_uuids, diff_uuids


def confirm_mapping(es, in_type, this_index_record):
    """
    The mapping put to ES can be incorrect, most likely due to residual
    items getting indexed at the time of index creation. This loop serves
    to find those problems and correct them, as well as provide more info
    for debugging the underlying issue.
    Returns number of iterations this took (0 means initial mapping was right)
    """
    mapping_check = False
    tries = 0
    while not mapping_check and tries < 5:
        found_mapping = es.indices.get_mapping(index=in_type).get(in_type, {}).get('mappings')
        found_map_json = json.dumps(found_mapping, sort_keys=True)
        this_map_json = json.dumps(this_index_record['mappings'], sort_keys=True)
        # es converts {'properties': {}} --> {'type': 'object'}
        this_map_json = this_map_json.replace('{"properties": {}}', '{"type": "object"}')
        if found_map_json == this_map_json:
            mapping_check = True
        else:
            count = es.count(index=in_type, doc_type=in_type).get('count', 0)
            log.info('___BAD MAPPING FOUND FOR %s. RETRYING___\nDocument count in that index is %s.'
                        % (in_type, count), collection=in_type, count=count, cat='bad mapping')
            es_safe_execute(es.indices.delete, index=in_type)
            # do not increment tries if an error arises from creating the index
            try:
                es_safe_execute(es.indices.create, index=in_type, body=this_index_record)
            except (TransportError, RequestError) as e:
                log.info('___COULD NOT CREATE INDEX FOR %s AS IT ALREADY EXISTS.\nError: %s\nRETRYING___'
                            % (in_type, str(e)), collection=in_type, cat='index already exists')
            else:
                tries += 1
            time.sleep(2)
    if not mapping_check:
        log.info('___MAPPING CORRECTION FAILED FOR %s___' % in_type, cat='correction', collection=in_type)
    return tries


def es_safe_execute(function, **kwargs):
    exec_count = 0
    while exec_count < 3:
        try:
            function(**kwargs)
        except ConnectionTimeout:
            exec_count += 1
            log.info('ES connection issue! Retrying.')
        else:
            return True
    return False


def flatten_and_sort_uuids(registry, uuids_to_index, item_order):
    """
    Flatten the input dict of sets (uuids_to_index) into a list that is ordered
    based off of item type, which is provided through item_order.
    item_order may be a list of item types (e.g. my_type) or item names
    (e.g. MyType)

    Args:
        reigstry: current Pyramid Registry
        uuids_to_index (set): keys are item_type and values are set of uuids
        item_order (list): string item types / item names to order by

    Returns:
        list: ordered uuids to index synchronously or queue for indexing
    """
    # process item_order to turn item names to item types
    proc_item_order = []
    for name_or_type in item_order:
        try:
            i_type = registry[COLLECTIONS][name_or_type].type_info.item_type
        except KeyError:
            # not an item name or type. Log error and exclude
            log.error('___Entry %s is not valid in mapping item_order. Skipping___' % name_or_type)
        else:
            proc_item_order.append(i_type)
    to_index_list = []

    def type_sort_key(i_type):
        """
        Simple helper fxn to sort collections by their index in item_order.
        If not in item_order, preserve order as-is
        """
        try:
            res = proc_item_order.index(i_type)
        except ValueError:
            res = 999
        return res

    # use type_sort_key fxn to sort + flatten uuids_to_index
    for itype in sorted(uuids_to_index.keys(), key=type_sort_key):
        to_index_list.extend(uuids_to_index[itype])
    return to_index_list


def run_indexing(app, indexing_uuids):
    """
    indexing_uuids is a set of uuids that should be reindexed. If global args
    are available, then this will spawn a new process to run indexing with.
    Otherwise, run with the current INDEXER
    """
    run_index_data(app, uuids=indexing_uuids)


def run(app, collections=None, dry_run=False, check_first=False, skip_indexing=False,
        index_diff=False, strict=False, sync_index=False, no_meta=False, print_count_only=False,
        purge_queue=False, bulk_meta=True, item_order=[]):
    """
    Run create_mapping. Has the following options:
    collections: run create mapping for the given list of item types only.
        If specific collections are set, meta will not be re-created.
    dry_run: if True, do not delete/create indices
    skip_indexing: if True, do not index ANYTHING with this run.
    check_first: if True, attempt to keep indices that have not changed mapping.
        If the document counts in the index and db do not match, delete index
        and queue all items in the index for reindexing.
    index_diff: if True, do NOT create/delete indices but identify any items
        that exist in db but not in es and reindex those.
        Takes precedence over check_first
    strict: if True, do not include associated items when considering what
        items to reindex. Only takes affect with index_diff or when specific
        item_types are specified, since otherwise a complete reindex will
        occur anyways.
    sync_index: if True, synchronously run reindexing rather than queueing.
    no_meta: if indexing all types (item_type not specified), will not
        re-create the meta index if set.
    print_count_only: if True, print counts for existing indices instead of
        queueing items for reindexing. Must to be used with check_first.
    purge_queue: if True, purge the contents of all relevant indexing queues.
        Is automatically done on a full indexing (no index_diff, check_first,
        or collections).
    bulk_meta: caches meta at the start of create_mapping and never queries or
        updates it again, until the end when all mappings are bulk loaded into meta
    item_order: provide a list of item types (e.g. my_type) or item names
        (e.g. MyType). Indexing/queueing order will be dictated by index in the
        list, such that the items at the front are indexed first.
    """
    from timeit import default_timer as timer
    overall_start = timer()
    registry = app.registry
    es = registry[ELASTIC_SEARCH]
    indexer_queue = registry[INDEXER_QUEUE]
    cat = 'start create mapping'

    # always overwrite telemetry id
    global log
    telemetry_id='cm_run_' + datetime.datetime.now().isoformat()
    log = log.bind(telemetry_id=telemetry_id)
    log.info('\n___CREATE-MAPPING___:\ncollections: %s\ncheck_first %s\n index_diff %s\n' %
                (collections, check_first, index_diff), cat=cat)
    log.info('\n___ES___:\n %s\n' % (str(es.cat.client)), cat=cat)
    log.info('\n___ES NODES___:\n %s\n' % (str(es.cat.nodes())), cat=cat)
    log.info('\n___ES HEALTH___:\n %s\n' % (str(es.cat.health())), cat=cat)
    log.info('\n___ES INDICES (PRE-MAPPING)___:\n %s\n' % str(es.cat.indices()), cat=cat)
    # keep track of uuids to be indexed after mapping is done.
    # Set of uuids for each item type; keyed by item type. Order for python < 3.6
    uuids_to_index = OrderedDict()
    total_reindex = (collections == None and not dry_run and not check_first and not index_diff and not print_count_only)

    if not collections:
        collections = list(registry[COLLECTIONS].by_item_type)
        # automatically add meta to start when going through all collections
        if not no_meta:
            collections = ['meta'] + collections

    # for bulk_meta, cache meta upfront, and only update it (with all mappings) at very end
    cached_indices = meta_bulk_actions = None
    if bulk_meta:
        cached_indices = cache_meta(es)
        meta_bulk_actions = []

    # if meta doesn't exist, always add it
    if not check_if_index_exists(es, 'meta', cached_indices) and 'meta' not in collections:
        collections = ['meta'] + collections

    # clear the indexer queue on a total reindex
    if total_reindex or purge_queue:
        log.info('___PURGING THE QUEUE AND CLEARING INDEXING RECORDS BEFORE MAPPING___\n', cat=cat)
        indexer_queue.clear_queue()
        # we also want to remove the 'indexing' index, which stores old records
        # it's not guaranteed to be there, though
        es_safe_execute(es.indices.delete, index='indexing', ignore=[400,404])

    # if 'indexing' index doesn't exist, initialize it with some basic settings
    # but no mapping. this is where indexing_records go
    if not check_if_index_exists(es, 'indexing'):
        idx_settings = {'settings': index_settings()}
        es_safe_execute(es.indices.create, index='indexing', body=idx_settings)

    greatest_mapping_time = {'collection': '', 'duration': 0}
    greatest_index_creation_time = {'collection': '', 'duration': 0}
    timings = {}
    log.info('\n___FOUND COLLECTIONS___:\n %s\n' % (str(collections)), cat=cat)
    for collection_name in collections:
        if collection_name == 'meta':
            # meta mapping just contains settings
            build_index(app, es, collection_name, META_MAPPING, uuids_to_index,
                        dry_run, check_first, index_diff, print_count_only,
                        cached_indices, meta_bulk_actions)
            # bail if we fail to make meta
            if not check_if_index_exists(es, 'meta'):
                log.info('\n___META NOT CREATED; CREATE MAPPING ABORTED___\n', cat=cat)
                return timings
            # only update this for bulk_meta setting
            if cached_indices:
                cached_indices['meta'] = 1
        else:
            start = timer()
            mapping = create_mapping_by_type(collection_name, registry)
            end = timer()
            mapping_time = end - start
            start = timer()
            build_index(app, es, collection_name, mapping, uuids_to_index,
                        dry_run, check_first, index_diff, print_count_only,
                        cached_indices, meta_bulk_actions)
            end = timer()
            index_time = end - start
            log.info('___FINISHED %s___\n' % (collection_name))
            log.info('___Mapping Time: %s  Index time %s ___\n' % (mapping_time, index_time),
                        cat='index mapping time', collection=collection_name, map_time=mapping_time,
                        index_time=index_time)
            if mapping_time > greatest_mapping_time['duration']:
                greatest_mapping_time['collection'] = collection_name
                greatest_mapping_time['duration'] = mapping_time
            if index_time > greatest_index_creation_time['duration']:
                greatest_index_creation_time['collection'] = collection_name
                greatest_index_creation_time['duration'] = index_time
            timings[collection_name] = {'mapping': mapping_time, 'index': index_time}

    # should be called only for bulk_meta setting
    if meta_bulk_actions:
        start = timer()
        bulk(es, meta_bulk_actions)
        end = timer()
        log.info("bulk update for meta took %s" % str(end-start), cat='bulk_meta time',
                    duration=str(end-start))

    overall_end = timer()
    cat = 'finished mapping'
    log.info('\n___ES INDICES (POST-MAPPING)___:\n %s\n' % (str(es.cat.indices())), cat=cat)
    log.info('\n___FINISHED CREATE-MAPPING___\n', cat=cat)


    log.info('\n___GREATEST MAPPING TIME: %s\n' % str(greatest_mapping_time),
                cat='max mapping time', **greatest_mapping_time)
    log.info('\n___GREATEST INDEX CREATION TIME: %s\n' % str(greatest_index_creation_time),
                cat='max index create time', **greatest_index_creation_time)
    log.info('\n___TIME FOR ALL COLLECTIONS: %s\n' % str(overall_end - overall_start),
                cat='overall mapping time', duration=str(overall_end - overall_start))
    if skip_indexing or print_count_only:
        return timings
    # now, queue items for indexing in the secondary queue
    # get a total list of all uuids to index among types for invalidation checking
    len_all_uuids = sum([len(uuids_to_index[i_type]) for i_type in uuids_to_index])
    if uuids_to_index:
        # only index (synchronously) if --sync-index option is used
        if sync_index:
            # using sync_index and NOT strict could cause issues with picking
            # up newly rev linked items. Print out an error and deal with it
            # for now
            if not strict:
                 # arbitrary large number, that hopefully is within ES limits
                if len_all_uuids > 30000 or total_reindex:
                    # get all the uuids from EVERY item type
                    for i_type in registry[COLLECTIONS].by_item_type:
                        uuids_to_index[i_type] = set(get_uuids_for_types(registry, types=[i_type]))
                else:
                    # find invalidated uuids for each index. Must concat all
                    # uuids over all types in uuids_to_index to do this
                    all_uuids_to_index = set(chain.from_iterable(uuids_to_index.values()))
                    for i_type in registry[COLLECTIONS].by_item_type:
                        if not check_if_index_exists(es, i_type):
                            continue
                        # must subtract the input uuids that are not of the given type
                        to_subtract = set(chain.from_iterable(
                            [v for k, v in uuids_to_index.items() if k != i_type]
                        ))
                        all_assc_uuids = find_uuids_for_indexing(registry, all_uuids_to_index, i_type)
                        uuids_to_index[i_type] = all_assc_uuids - to_subtract
                log.error('___SYNC INDEXING WITH STRICT=FALSE MAY CAUSE REV_LINK INCONSISTENCY___')
            # sort by-type uuids into one list and index synchronously
            to_index_list = flatten_and_sort_uuids(app.registry, uuids_to_index, item_order)
            log.info('\n___UUIDS TO INDEX (SYNC)___: %s\n' % len(to_index_list),
                        cat='uuids to index', count=len(to_index_list))
            run_indexing(app, to_index_list)
        else:
            # if non-strict and attempting to reindex a ton, it is faster
            # just to strictly reindex all items
            use_strict = strict or total_reindex
            if len_all_uuids > 30000 and not use_strict:
                log.error('___MAPPING ALL ITEMS WITH STRICT=TRUE TO SAVE TIME___')
                # get all the uuids from EVERY item type
                for i_type in registry[COLLECTIONS].by_item_type:
                    uuids_to_index[i_type] = set(get_uuids_for_types(registry, types=[i_type]))
                use_strict = True
            # sort by-type uuids into one list and queue for indexing
            to_index_list = flatten_and_sort_uuids(app.registry, uuids_to_index, item_order)
            log.info('\n___UUIDS TO INDEX (QUEUED)___: %s\n' % len(to_index_list),
                        cat='uuids to index', count=len(to_index_list))
            indexer_queue.add_uuids(app.registry, to_index_list, strict=use_strict,
                                    target_queue='secondary', telemetry_id=telemetry_id)
    return timings


def cache_meta(es):
    ''' get what we know from ES about the state of our indexes'''

    existing_indices = es.cat.indices(h='index,docs.count')
    # determine which collections exist
    indices_list = existing_indices.split('\n')
    # indices call leaves some trailing stuff
    if indices_list[-1] == '': indices_list.pop()
    indices = {}
    for index in indices_list:
        if index:
            name, count = index.split()
            indices[name] = {'count': count}

    # store all existing mappings
    # we no longer create any mapping for meta.. so it's not searchable
    '''if 'meta' in indices:
        meta_idx = es.search(index='meta', body={'query': {'match_all': {}}})
        for idx in meta_idx['hits']['hits']:
            if idx.get('_id') and idx['_id'] in indices:
                indices[idx['_id']]['mapping'] = idx['_source']
    '''
    # now indices should be all index that xisted with size and mapping (mapping includes settings)
    return indices


def main():
    parser = argparse.ArgumentParser(
        description="Create Elasticsearch mapping", epilog=EPILOG,
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    parser.add_argument('config_uri', help="path to configfile")
    parser.add_argument('--app-name', help="Pyramid app name in configfile")
    parser.add_argument('--item-type', action='append', help="Item type")
    parser.add_argument('--dry-run', action='store_true',
                        help="Don't post to ES, just print")
    parser.add_argument('--check-first', action='store_true',
                        help="check if index exists first before attempting creation")
    parser.add_argument('--skip-indexing', action='store_true',
                        help="skip all indexing if set")
    parser.add_argument('--index-diff', action='store_true',
                        help="reindex any items in the db but not es store for all/given collections")
    parser.add_argument('--strict', action='store_true',
                        help="used with check_first in combination with item-type. Only index the given types (ignore associated items). Advanced users only")
    parser.add_argument('--sync-index', action='store_true',
                        help="add to cause reindexing to occur synchronously instead of using the meta uuid_store")
    parser.add_argument('--no-meta', action='store_true',
                        help="add to disregard the meta index")
    parser.add_argument('--print-count-only', action='store_true',
                        help="use with check_first to only print counts")
    parser.add_argument('--purge-queue', action='store_true',
                        help="purge the contents of all queues, regardless of run mode")

    args = parser.parse_args()

    #logging.basicConfig()
    app = get_app(args.config_uri, args.app_name)


    # Loading app will have configured from config file. Reconfigure here:
    set_logging(app.registry.settings.get('elasticsearch.server'),
                app.registry.settings.get('production'), level=logging.INFO)
    #global log
    #log = structlog.get_logger(__name__)

    uuids = run(app, args.item_type, args.dry_run, args.check_first, args.skip_indexing,
                args.index_diff, args.strict, args.sync_index, args.no_meta,
                args.print_count_only, args.purge_queue)
    return


if __name__ == '__main__':
    main()
