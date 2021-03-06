# See http://docs.pylonsproject.org/projects/pyramid/en/latest/narr/resources.html
import logging
from collections import Mapping
from copy import deepcopy
from pyramid.decorator import reify
from pyramid.httpexceptions import HTTPInternalServerError
from pyramid.security import (
    Allow,
    Everyone,
)
from pyramid.traversal import (
    resource_path,
    traverse,
)
from .calculated import (
    calculate_properties,
    calculated_property,
)
from .interfaces import (
    COLLECTIONS,
    CONNECTION,
    ROOT,
    TYPES,
    UPGRADER,
)
from .validation import ValidationFailure
from .util import (
    ensurelist,
    simple_path_ids,
    uuid_to_path
)
from past.builtins import basestring
from .util import add_default_embeds

logger = logging.getLogger(__name__)


def includeme(config):
    config.scan(__name__)


class Resource(object):

    @calculated_property(name='@id', schema={
        "title": "ID",
        "type": "string",
    })
    def jsonld_id(self, request):
        return request.resource_path(self)

    @calculated_property(name='@context', category='page')
    def jsonld_context(self, request):
        return request.route_path('jsonld_context')

    @calculated_property(category='page')
    def actions(self, request):
        actions = calculate_properties(self, request, category='action')
        if actions:
            return list(actions.values())


class Root(Resource):
    __name__ = ''
    __parent__ = None
    __acl__ = [
        (Allow, 'remoteuser.INDEXER', ['view', 'list', 'index']),
        (Allow, 'remoteuser.EMBED', ['view', 'expand', 'audit']),
        (Allow, Everyone, ['visible_for_edit']),
    ]

    def __init__(self, registry):
        self.registry = registry

    @reify
    def connection(self):
        return self.registry[CONNECTION]

    @reify
    def collections(self):
        return self.registry[COLLECTIONS]

    def __getitem__(self, name):
        try:
            resource = self.get(name)
        except KeyError:
            # Just in case we get an unexpected KeyError
            # FIXME: exception logging.
            raise HTTPInternalServerError('Traversal raised KeyError')
        if resource is None:
            raise KeyError(name)
        return resource

    def __contains__(self, name):
        return self.get(name, None) is not None

    def get(self, name, default=None):
        resource = self.collections.get(name, None)
        if resource is not None:
            return resource
        resource = self.connection.get_by_uuid(name, None)
        if resource is not None:
            return resource
        return default

    def __json__(self, request=None):
        return self.properties.copy()

    @calculated_property(name='@type', schema={
        "title": "Type",
        "type": "array",
        "items": {
            "type": "string",
        },
    })
    def jsonld_type(self):
        return ['Portal']


class AbstractCollection(Resource, Mapping):
    properties = {}
    unique_key = None

    def __init__(self, registry, name, type_info, properties=None, acl=None, unique_key=None):
        self.registry = registry
        self.__name__ = name
        self.type_info = type_info
        if properties is not None:
            self.properties = properties
        if acl is not None:
            self.__acl__ = acl
        if unique_key is not None:
            self.unique_key = unique_key

    @reify
    def connection(self):
        return self.registry[CONNECTION]

    @reify
    def __parent__(self):
        return self.registry[ROOT]

    def __getitem__(self, name):
        try:
            item = self.get(name)
        except KeyError:
            # Just in case we get an unexpected KeyError
            # FIXME: exception logging.
            raise HTTPInternalServerError('Traversal raised KeyError')
        if item is None:
            raise KeyError(name)
        return item

    def __iter__(self):
        for uuid in self.connection.__iter__(*self.type_info.subtypes):
            yield uuid

    def __len__(self):
        return self.connection.__len__(*self.type_info.subtypes)

    def __hash__(self):
        return object.__hash__(self)

    def __eq__(self, other):
        return self is other

    def _allow_contained(self, resource):
        return resource.__parent__ is self or \
            resource.type_info.name in resource.type_info.subtypes

    def get(self, name, default=None):
        resource = self.connection.get_by_uuid(name, None)
        if resource is not None:
            if not self._allow_contained(resource):
                return default
            return resource
        if self.unique_key is not None:
            resource = self.connection.get_by_unique_key(self.unique_key, name)
            if resource is not None:
                if not self._allow_contained(resource):
                    return default
                return resource
        return default

    def iter_no_subtypes(self):
        """
        Make a generator that yields all items in the collection, but not
        subtypes
        """
        for uuid in self.connection.__iter__(self.type_info.item_type):
            yield uuid

    def __json__(self, request):
        return self.properties.copy()

    @calculated_property(name='@type', schema={
        "title": "Type",
        "type": "array",
        "items": {
            "type": "string",
        },
    })
    def jsonld_type(self):
        return [
            '{type_name}Collection'.format(type_name=self.type_info.name),
            'Collection',
        ]


class Collection(AbstractCollection):
    ''' Separate class so add views do not apply to AbstractCollection '''


class Item(Resource):
    item_type = None
    base_types = ['Item']
    name_key = None
    rev = {}
    aggregated_items = {}
    embedded_list = []
    filtered_rev_statuses = ()
    audit_inherit = None
    schema = None
    AbstractCollection = AbstractCollection
    Collection = Collection

    def __init__(self, registry, model):
        self.registry = registry
        self.model = model

    def __repr__(self):
        return '<%s at %s>' % (type(self).__name__, resource_path(self))

    @reify
    def type_info(self):
        return self.registry[TYPES][type(self)]

    @reify
    def collection(self):
        collections = self.registry[COLLECTIONS]
        return collections[self.type_info.name]

    @property
    def __parent__(self):
        return self.collection

    @property
    def __name__(self):

        if self.name_key is None:
            return str(self.uuid)
        return self.properties.get(self.name_key, None) or str(self.uuid)

    @property
    def properties(self):
        return self.model.properties

    @property
    def propsheets(self):
        return self.model.propsheets

    @property
    def uuid(self):
        return self.model.uuid

    @property
    def sid(self):
        return self.model.sid

    def links(self, properties):
        return {
            path: set(simple_path_ids(properties, path))
            for path in self.type_info.schema_links
        }

    def get_rev_links(self, request, name):
        """
        Return a list of uuid rev_links for the given rev name (in self.rev)
        from the given item.

        Args:
            request: current Request
            name (str): name of the rev (must be in self.rev)

        Returns:
            list of str uuids of the given rev_link
        """
        types = self.registry[TYPES]
        type_name, rel = self.rev[name]
        types = types[type_name].subtypes
        # if we are indexing, update the following request attributes
        return self.registry[CONNECTION].get_rev_links(self.model, rel, *types)

    def get_filtered_rev_links(self, request, name):
        """
        Run get_rev_links, but only return items that do not have a status
        in self.filtered_rev_statuses (a tuple defined on the Item)
        If we are indexing, add rev_link info to _rev_linked_uuids_by_item.

        Args:
            request: current Request
            name (str): name of the rev (must be in self.rev)

        Returns:
            list of str uuids of the given rev_link, filtered by status
        """
        # Consider caching rev links on the request? Would save DB requests
        # May not be worth it because they are quite fast
        rev_uuids = self.get_rev_links(request, name)
        filtered_uuids = [
            str(rev_id) for rev_id in rev_uuids
            if traverse(request.root, str(rev_id))['context'].__json__(request).get('status')
            not in self.filtered_rev_statuses
        ]
        if getattr(request, '_indexing_view', False) is True:
            to_update = {name: filtered_uuids}
            if str(self.uuid) in request._rev_linked_uuids_by_item:
                request._rev_linked_uuids_by_item[str(self.uuid)].update(to_update)
            else:
                request._rev_linked_uuids_by_item[str(self.uuid)] = to_update
        return filtered_uuids

    def unique_keys(self, properties):
        return {
            name: [v for prop in props for v in ensurelist(properties.get(prop, ()))]
            for name, props in self.type_info.schema_keys.items()
        }

    def upgrade_properties(self):
        """
        Calls the upgrader on the Item if properties.schema_version is not current
        """
        try:
            properties = deepcopy(self.properties)
        except KeyError:
            # don't fail if we try to upgrade properties on something not there yet
            return None
        current_version = properties.get('schema_version', '')
        target_version = self.type_info.schema_version
        if target_version is not None and current_version != target_version:
            upgrader = self.registry[UPGRADER]
            try:
                properties = upgrader.upgrade(
                    self.type_info.name, properties, current_version, target_version,
                    context=self, registry=self.registry)
            except RuntimeError:
                raise
            except Exception:
                logger.warning(
                    'Unable to upgrade %s from %r to %r',
                    resource_path(self.__parent__, self.uuid),
                    current_version, target_version, exc_info=True)
        return properties

    def __json__(self, request):
        return self.upgrade_properties()

    def item_with_links(self, request):
        # This works from the schema rather than the links table
        # so that upgrade on GET can work.
        # context.__json__ CALLS THE UPGRADER (upgrade_properties)
        properties = self.__json__(request)
        for path in self.type_info.schema_links:
            uuid_to_path(request, properties, path)

        # if indexing, add the uuid of this object to request._linked_uuids
        # and add the sid to _sid_cache if not already present
        if getattr(request, '_indexing_view', False) is True:
            request._linked_uuids.add(str(self.uuid))
            if str(self.uuid) not in request._sid_cache:
                request._sid_cache[str(self.uuid)] = self.sid
        return properties

    def __resource_url__(self, request, info):
        return None

    @classmethod
    def create(cls, registry, uuid, properties, sheets=None):
        '''
        This class method is called in crud_views.py - `collection_add` (API endpoint) > `create_item` (method) > `type_info.factory.create` (this class method)

        This method instantiates a new Item class instance from provided `uuid` and `properties`,
        then runs the `_update` (instance method) to save the Item to the database.
        '''
        model = registry[CONNECTION].create(cls.__name__, uuid)
        item_instance = cls(registry, model)
        item_instance._update(properties, sheets)
        return item_instance

    def validate_path_characters(self, field, value):
        """
        Check that the field with given value does not contain any characters
        that interfere with the resource_path. Currently, we allow all
        alphanumeric characters and few others
        """
        also_allowed = ['_', '-', ':', ',', '.', ' ', '@']
        if not isinstance(value, basestring):
            raise ValueError('Identifying property %s must be a string. Value: %s' % (field, value))
        forbidden = [char for char in value
                     if (not char.isalnum() and char not in also_allowed)]
        if any(forbidden):
            msg = ("Forbidden character(s) %s are not allowed in field: %s. Value: %s"
                   % (set(forbidden), field, value))
            raise ValidationFailure('body', [], msg)

    def update(self, properties, sheets=None):
        '''Alias of _update, called in crud_views.py - `update_item` (method)'''
        self._update(properties, sheets)

    def _update(self, properties, sheets=None):
        '''
        This instance method is called in Item.create (classmethod) as well as in crud_views.py - `item_edit` (API endpoint) > `update_item` (method) > `context.update` (instance method).

        This method is used to assert lack of duplicate unique keys in database and then to perform database update of `properties` (dict).

        Optionally define this method in inherited classes to extend `properties` on Item updates.
        '''
        unique_keys = None
        links = None
        if properties is not None:
            if 'uuid' in properties:
                properties = properties.copy()
                del properties['uuid']

            # validation on name key and unique keys
            nk_val = properties.get(self.name_key, '')
            self.validate_path_characters(self.name_key, nk_val)

            unique_keys = self.unique_keys(properties)
            for k, values in unique_keys.items():
                if len(set(values)) != len(values):
                    msg = "Duplicate keys for %r: %r" % (k, values)
                    raise ValidationFailure('body', [], msg)
                for uk_val in values:
                    self.validate_path_characters(k, uk_val)

            links = self.links(properties)

        # actually propogate the update to the DB
        connection = self.registry[CONNECTION]
        connection.update(self.model, properties, sheets, unique_keys, links)

    @reify
    def embedded(self):
        """
        Use the embedded_list defined for the individual types to create the
        embedded attribute through expansion using add_default_embeds
        """
        total_schema = self.schema.get('properties', {}).copy()
        calc_props_schema = {}
        types = self.registry[TYPES]
        if self.registry['calculated_properties']:
            for calc_props_key, calc_props_val in self.registry['calculated_properties'].props_for(self).items():
                if calc_props_val.schema:
                    calc_props_schema[calc_props_key] = calc_props_val.schema
        total_schema.update(calc_props_schema)
        this_type = self.type_info.item_type
        return add_default_embeds(this_type, types, self.embedded_list, total_schema)

    @calculated_property(name='@type', schema={
        "title": "Type",
        "type": "array",
        "items": {
            "type": "string",
        },
    })
    def jsonld_type(self):
        return [self.type_info.name] + self.base_types

    @calculated_property(name='uuid')
    def prop_uuid(self):
        return str(self.uuid)

    @calculated_property(schema={
        "title": "principals_allowed",
        "description": "Calculated permissions used for ES filtering",
        "type": "object",
        'properties': {
            'view': {
                'type': 'string'
            },
            'edit': {
                'type': 'string'
            },
            'audit': {
                'type': 'string'
            }
        }
    })
    def principals_allowed(self):
        # importing at top of file requires many more relative imports
        from .authentication import calc_principals
        principals = calc_principals(self)
        return principals

    @calculated_property(schema={
        "title": "Display Title",
        "description": "A calculated title",
        "type": "string"
    })
    def display_title(self):
        return str(self.uuid)
