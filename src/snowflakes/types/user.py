# -*- coding: utf-8 -*-
import uuid
from pyramid.httpexceptions import HTTPUnprocessableEntity
from pyramid.view import (
    view_config,
)
from pyramid.security import (
    Allow,
    Deny,
    Everyone,
)
from .base import (
    Item,
    paths_filtered_by_status,
)
from snovault import (
    CONNECTION,
    DBSESSION,
    calculated_property,
    collection,
    load_schema,
)
from snovault.calculated import calculate_properties
from snovault.resource_views import item_view_page
from snovault.crud_views import collection_add
from snovault.schema_utils import validate_request
from snovault.storage import User as AuthUser


ONLY_ADMIN_VIEW_DETAILS = [
    (Allow, 'group.admin', ['view', 'view_details', 'edit']),
    (Allow, 'group.read-only-admin', ['view', 'view_details']),
    (Allow, 'remoteuser.INDEXER', ['view']),
    (Allow, 'remoteuser.EMBED', ['view']),
    (Deny, Everyone, ['view', 'view_details', 'edit']),
]

USER_ALLOW_CURRENT = [
    (Allow, Everyone, 'view'),
] + ONLY_ADMIN_VIEW_DETAILS

USER_DELETED = [
    (Deny, Everyone, 'visible_for_edit')
] + ONLY_ADMIN_VIEW_DETAILS


@collection(
    name='users',
    unique_key='user:email',
    properties={
        'title': 'Snowflake  Users',
        'description': 'Listing of current Snowflake users',
    },
    acl=[])
class User(Item):
    item_type = 'user'
    schema = load_schema('snowflakes:schemas/user.json')
    # Avoid access_keys reverse link so editing access keys does not reindex content.
    embedded_list = [
        'lab.*',
        'submits_for.@id'
    ]
    STATUS_ACL = {
        'current': [(Allow, 'role.owner', ['edit', 'view_details'])] + USER_ALLOW_CURRENT,
        'deleted': USER_DELETED,
        'replaced': USER_DELETED,
        'disabled': ONLY_ADMIN_VIEW_DETAILS,
    }

    @calculated_property(schema={
        "title": "Title",
        "type": "string",
    })
    def title(self, first_name, last_name):
        return u'{} {}'.format(first_name, last_name)

    def __ac_local_roles__(self):
        owner = 'userid.%s' % self.uuid
        return {owner: 'role.owner'}

    def _update(self, properties, sheets=None):
        '''
        overwritting this so as to create an associated 'web user'
        for each user object, if it doesn't have on already
        '''
        super(User, self)._update(properties, sheets)

        # after we save if we don't have AuthUser create it
        db = self.registry[DBSESSION]
        email = properties['email']
        if db.query(AuthUser).filter_by(email=email).first():
            return

        name ="%s %s" % (properties['first_name'], properties['last_name'])
        # we don't keep passwords in our user schema, so just create a random one here
        pwd = str(uuid.uuid4())

        # this is executed in an existing session, so it should get saved eventually
        auth_user = AuthUser(email, pwd, name)
        db.add(auth_user)

    @calculated_property(schema={
        "title": "Access Keys",
        "type": "array",
        "items": {
            "type": ['string', 'object'],
            "linkTo": "AccessKey"
        }
    }, category='page')
    def access_keys(self, request):
        if not request.has_permission('view_details'):
            return []
        key_coll = self.registry['collections']['AccessKey']
        # need to handle both esstorage and db storage results
        uuids = [str(uuid) for uuid in key_coll]
        acc_keys = [request.embed('/', uuid, '@@object')
                    for uuid in paths_filtered_by_status(request, uuids)]
        my_keys = [acc_key for acc_key in acc_keys if acc_key['user'] == request.path]
        if my_keys:
            return [key for key in my_keys if key['status'] not in ('deleted', 'replaced')]
        else:
            return []


@view_config(context=User, permission='view', request_method='GET', name='page')
def user_page_view(context, request):
    """smth."""
    properties = item_view_page(context, request)
    if not request.has_permission('view_details'):
        filtered = {}
        for key in ['@id', '@type', 'uuid', 'lab', 'title', 'display_title']:
            try:
                filtered[key] = properties[key]
            except KeyError:
                pass
        return filtered
    return properties


@view_config(context=User.Collection, permission='add', request_method='POST',
             physical_path="/users")
def user_add(context,request):
    ''' if we have a password in our request, create and auth entry
     for the user as well
     '''

    #do we have valid data
    pwd = request.json.get('password', None)
    pwd_less_data = request.json.copy()

    if pwd is not None:
        del pwd_less_data['password']

    validate_request(context.type_info.schema, request, pwd_less_data)

    if request.errors:
        return HTTPUnprocessableEntity(json={'errors':request.errors},
                                     content_type='application/json')

    # this will create an AuthUser with random password
    result = collection_add(context, request)
    if result:
        email = request.json.get('email')
        pwd = request.json.get('password', None)

        if pwd is not None:
            # now update the password
            db = request.registry['dbsession']
            auth_user = db.query(AuthUser).filter_by(email=email).first()
            auth_user.password = pwd

            import transaction
            transaction.commit()

    return result


@calculated_property(context=User, category='user_action')
def impersonate(request):
    # This is assuming the user_action calculated properties
    # will only be fetched from the current_user view,
    # which ensures that the user represented by 'context' is also an effective principal
    if request.has_permission('impersonate'):
        return {
            'id': 'impersonate',
            'title': 'Impersonate User…',
            'href': '/#!impersonate-user',
        }


@calculated_property(context=User, category='user_action')
def profile(context, request):
    return {
        'id': 'profile',
        'title': 'Profile',
        'href': request.resource_path(context),
    }


@calculated_property(context=User, category='user_action')
def signout(context, request):
    return {
        'id': 'signout',
        'title': 'Sign out',
        'trigger': 'logout',
    }
