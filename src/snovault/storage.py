
from pyramid.httpexceptions import HTTPConflict
from sqlalchemy import (
    Column,
    DDL,
    ForeignKey,
    bindparam,
    event,
    func,
    null,
    orm,
    schema,
    text,
    types,
)
from sqlalchemy.dialects import postgresql
from sqlalchemy.dialects.postgresql import JSONB as JSON
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext import baked
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import (
    collections,
    backref
)
from sqlalchemy.orm.exc import (
    FlushError,
    NoResultFound,
    MultipleResultsFound,
)
from .interfaces import (
    BLOBS,
    DBSESSION,
    STORAGE,
)
import boto3
import transaction
import uuid
import time


_DBSESSION = None


def includeme(config):
    registry = config.registry
    registry[STORAGE] = RDBStorage(registry[DBSESSION])
    global _DBSESSION
    _DBSESSION = registry[DBSESSION]
    if registry.settings.get('blob_bucket'):
        registry[BLOBS] = S3BlobStorage(
            registry.settings['blob_bucket'],
        )
    else:
        registry[BLOBS] = RDBBlobStorage(registry[DBSESSION])


Base = declarative_base()

# baked queries allow for caching of query construction to save Python overhead
bakery = baked.bakery()
baked_query_resource = bakery(lambda session: session.query(Resource))
baked_query_unique_key = bakery(
    lambda session: session.query(Key).options(
        orm.joinedload_all(
            Key.resource,
            Resource.data,
            CurrentPropertySheet.propsheet,
            innerjoin=True,
        ),
    ).filter(Key.name == bindparam('name'), Key.value == bindparam('value'))
)
# Baked queries can be used with expanding params (lists)
# https://docs.sqlalchemy.org/en/latest/orm/extensions/baked.html#baked-in
baked_query_sids = bakery(lambda session: session.query(CurrentPropertySheet))
baked_query_sids += lambda q: q.filter(CurrentPropertySheet.rid.in_(bindparam('rids', expanding=True)))


class RDBStorage(object):
    batchsize = 1000

    def __init__(self, DBSession):
        self.DBSession = DBSession

    @property
    def write(self):
        return self

    @property
    def read(self):
        return self

    def get_by_uuid(self, rid, default=None):
        session = self.DBSession()
        model = baked_query_resource(session).get(uuid.UUID(rid))
        if model is None:
            return default
        return model

    def get_by_uuid_direct(self, rid, item_type, default=None):
        """
        This method is meant to only work with ES, so return None (default)
        for the DB implementation

        Args:
            rid (str): item rid (uuid)
            item_type (str): item_type of the target resource (Item.item_type)
            default: View to return on a failure. Defaults to None.

        Returns:
            default
        """
        return default

    def get_by_unique_key(self, unique_key, name, default=None):
        session = self.DBSession()
        try:
            key = baked_query_unique_key(session).params(name=unique_key, value=name).one()
        except NoResultFound:
            return default
        else:
            return key.resource

    def get_by_json(self, key, value, item_type, default=None):
        session = self.DBSession()
        try:
            # baked query seem to not work with json
            query = (session.query(CurrentPropertySheet)
                     .join(CurrentPropertySheet.propsheet, CurrentPropertySheet.resource)
                     .filter(Resource.item_type == item_type,
                             PropertySheet.properties[key].astext == value)
                     )
            data = query.one()
            return data.resource
        except (NoResultFound, MultipleResultsFound):
            return default

    def get_rev_links(self, model, rel, *item_types):
        if item_types:
            return [
                link.source_rid for link in model.revs
                if link.rel == rel and link.source.item_type in item_types]
        else:
            return [link.source_rid for link in model.revs if link.rel == rel]

    def get_sids_by_uuids(self, rids):
        """
        Take a list of rids and return the sids from all of them using the
        CurrentPropertySheet table. This follows the convention of only using
        Resources with the default '' name.

        Args:
            rids (list): list of string rids (uuids)

        Returns:
            dict keyed by rid with integer sid values
        """
        if not rids:
            return []
        session = self.DBSession()
        results = baked_query_sids(session).params(rids=rids).all()
        # check res.name to skip sids for supplementary rows, like 'downloads'
        data = {str(res.rid): res.sid for res in results if res.name == ''}
        return data

    def __iter__(self, *item_types):
        session = self.DBSession()
        query = session.query(Resource.rid)

        if item_types:
            query = query.filter(
                Resource.item_type.in_(item_types)
            )

        for rid, in query.yield_per(self.batchsize):
            yield rid

    def __len__(self, *item_types):
        session = self.DBSession()
        query = session.query(Resource.rid)

        if item_types:
            query = query.filter(
                Resource.item_type.in_(item_types)
            )

        return query.count()

    def create(self, item_type, rid):
        return Resource(item_type, rid=rid)

    def update(self, model, properties=None, sheets=None, unique_keys=None, links=None):
        session = self.DBSession()
        sp = session.begin_nested()
        try:
            session.add(model)
            self._update_properties(model, properties, sheets)
            if links is not None:
                self._update_rels(model, links)
            if unique_keys is not None:
                keys_add, keys_remove = self._update_keys(model, unique_keys)
            sp.commit()
        except (IntegrityError, FlushError):
            sp.rollback()
        else:
            return

        # Try again more carefully
        try:
            session.add(model)
            self._update_properties(model, properties, sheets)
            if links is not None:
                self._update_rels(model, links)
            session.flush()
        except (IntegrityError, FlushError):
            msg = 'UUID conflict'
            raise HTTPConflict(msg)
        assert unique_keys is not None
        conflicts = [pk for pk in keys_add if session.query(Key).get(pk) is not None]
        assert conflicts
        msg = 'Keys conflict: %r' % conflicts
        raise HTTPConflict(msg)

    def purge_uuid(self, rid):
        # WARNING USE WITH CARE PERMANENTLY DELETES RESOURCES
        session = self.DBSession()
        sp = session.begin_nested()
        model = self.get_by_uuid(rid)
        try:
            for current_propsheet in model.data.values():
                # delete the propsheet history
                for propsheet in current_propsheet.history:
                    session.delete(propsheet)
                    # now delete the currentPropsheet
                    session.delete(current_propsheet)
                # now delete the resource, keys and links(via cascade)
            session.delete(model)
            sp.commit()
        except Exception as e:
            sp.rollback()
            raise e

    def _update_properties(self, model, properties, sheets=None):
        if properties is not None:
            model.propsheets[''] = properties
        if sheets is not None:
            for key, value in sheets.items():
                model.propsheets[key] = value

    def _update_keys(self, model, unique_keys):
        keys_set = {(k, v) for k, values in unique_keys.items() for v in values}

        existing = {
            (key.name, key.value)
            for key in model.unique_keys
        }

        to_remove = existing - keys_set
        to_add = keys_set - existing

        session = self.DBSession()
        for pk in to_remove:
            key = session.query(Key).get(pk)
            session.delete(key)

        for name, value in to_add:
            key = Key(rid=model.rid, name=name, value=value)
            session.add(key)

        return to_add, to_remove

    def _update_rels(self, model, links):
        session = self.DBSession()
        source = model.rid

        rels = {(k, uuid.UUID(target)) for k, targets in links.items() for target in targets}

        existing = {
            (link.rel, link.target_rid)
            for link in model.rels
        }

        to_remove = existing - rels
        to_add = rels - existing

        for rel, target in to_remove:
            link = session.query(Link).get((source, rel, target))
            session.delete(link)

        for rel, target in to_add:
            link = Link(source_rid=source, rel=rel, target_rid=target)
            session.add(link)

        return to_add, to_remove


class UUID(types.TypeDecorator):
    """Platform-independent UUID type.

    Uses Postgresql's UUID type, otherwise uses
    CHAR(32), storing as stringified hex values.

    """
    impl = types.CHAR

    def load_dialect_impl(self, dialect):
        if dialect.name == 'postgresql':
            return dialect.type_descriptor(postgresql.UUID())
        else:
            return dialect.type_descriptor(types.CHAR(32))

    def process_bind_param(self, value, dialect):
        if value is None:
            return value
        elif dialect.name == 'postgresql':
            return str(value)
        else:
            if not isinstance(value, uuid.UUID):
                return uuid.UUID(value).hex
            else:
                return value.hex

    def process_result_value(self, value, dialect):
        if value is None:
            return value
        else:
            return uuid.UUID(value)


class RDBBlobStorage(object):
    def __init__(self, DBSession):
        self.DBSession = DBSession

    def store_blob(self, data, download_meta, blob_id=None):
        if blob_id is None:
            blob_id = uuid.uuid4()
        elif isinstance(blob_id, str):
            blob_id = uuid.UUID(blob_id)
        session = self.DBSession()
        blob = Blob(blob_id=blob_id, data=data)
        session.add(blob)
        download_meta['blob_id'] = str(blob_id)

    def get_blob(self, download_meta):
        blob_id = download_meta['blob_id']
        if isinstance(blob_id, str):
            blob_id = uuid.UUID(blob_id)
        session = self.DBSession()
        blob = session.query(Blob).get(blob_id)
        return blob.data


class S3BlobStorage(object):
    def __init__(self, bucket):
        self.bucket = bucket
        session = boto3.session.Session(region_name='us-east-1')
        self.s3 = session.client('s3')

    def store_blob(self, data, download_meta, blob_id=None):
        '''
        create a new s3 key = blob_id
        upload the contents and return the meta in download_meta
        '''
        if blob_id is None:
            blob_id = str(uuid.uuid4())

        content_type = download_meta.get('type','binary/octet-stream')

        self.s3.put_object(Bucket=self.bucket,
                           Key=blob_id,
                           Body=data,
                           ContentType=content_type
                           )
        download_meta['bucket'] = self.bucket
        download_meta['key'] = blob_id
        download_meta['blob_id'] = str(blob_id)

    def _get_bucket_key(self, download_meta):
        # Assume files have been migrated
        if 'bucket' in download_meta:
            return download_meta['bucket'], download_meta['key']
        else:
            return self.bucket, download_meta['blob_id']

    def get_blob_url(self, download_meta):
        bucket_name, key = self._get_bucket_key(download_meta)

        location = self.s3.generate_presigned_url(
            ClientMethod='get_object',
            ExpiresIn=36*60*60,
            Params={'Bucket': bucket_name, 'Key': key})
        return location

    def get_blob(self, download_meta):
        bucket_name, key = self._get_bucket_key(download_meta)

        response = self.s3.get_object(Bucket=bucket_name,
                                 Key=key)
        return response['Body'].read().decode()


class Key(Base):
    ''' indexed unique tables for accessions and other unique keys
    '''
    __tablename__ = 'keys'

    # typically the field that is unique, i.e. accession
    # might be prefixed with a namespace for per name unique values
    name = Column(types.String, primary_key=True)
    # the unique value
    value = Column(types.String, primary_key=True)

    rid = Column(UUID, ForeignKey('resources.rid'),
                 nullable=False, index=True)

    # Be explicit about dependencies to the ORM layer
    resource = orm.relationship('Resource',
                                backref=backref('unique_keys', cascade='all, delete-orphan'))


class Link(Base):
    """ indexed relations
    """
    __tablename__ = 'links'
    source_rid = Column(
        'source', UUID, ForeignKey('resources.rid'), primary_key=True)
    rel = Column(types.String, primary_key=True)
    target_rid = Column(
        'target', UUID, ForeignKey('resources.rid'), primary_key=True,
        index=True)  # Single column index for reverse lookup

    source = orm.relationship(
        'Resource', foreign_keys=[source_rid], backref=backref('rels',
                                                               cascade='all, delete-orphan'))
    target = orm.relationship(
        'Resource', foreign_keys=[target_rid], backref=backref('revs',
                                                               cascade='all, delete-orphan'))


class PropertySheet(Base):
    '''A triple describing a resource
    '''
    __tablename__ = 'propsheets'
    __table_args__ = (
        schema.ForeignKeyConstraint(
            ['rid', 'name'],
            ['current_propsheets.rid', 'current_propsheets.name'],
            name='fk_property_sheets_rid_name', use_alter=True,
            deferrable=True, initially='DEFERRED',
        ),
    )
    # The sid column also serves as the order.
    sid = Column(types.Integer, autoincrement=True, primary_key=True)
    rid = Column(UUID,
                 ForeignKey('resources.rid',
                            deferrable=True,
                            initially='DEFERRED'),
                 nullable=False)
    name = Column(types.String, nullable=False)
    properties = Column(JSON)
    tid = Column(UUID,
                 ForeignKey('transactions.tid',
                            deferrable=True,
                            initially='DEFERRED'),
                 nullable=False)
    resource = orm.relationship('Resource')
    transaction = orm.relationship('TransactionRecord')


class CurrentPropertySheet(Base):
    __tablename__ = 'current_propsheets'
    rid = Column(UUID, ForeignKey('resources.rid'),
                 nullable=False, primary_key=True)
    name = Column(types.String, nullable=False, primary_key=True)
    sid = Column(types.Integer,
                 ForeignKey('propsheets.sid'), nullable=False)
    propsheet = orm.relationship(
        'PropertySheet', lazy='joined', innerjoin=True,
        primaryjoin="CurrentPropertySheet.sid==PropertySheet.sid",
    )
    history = orm.relationship(
        'PropertySheet', order_by=PropertySheet.sid,
        post_update=True,  # Break cyclic dependency
        primaryjoin="""and_(CurrentPropertySheet.rid==PropertySheet.rid,
                    CurrentPropertySheet.name==PropertySheet.name)""",
        viewonly=True,
    )
    resource = orm.relationship('Resource')
    __mapper_args__ = {'confirm_deleted_rows': False}


class Resource(Base):
    '''Resources are described by multiple propsheets
    '''
    __tablename__ = 'resources'
    rid = Column(UUID, primary_key=True)
    item_type = Column(types.String, nullable=False)
    data = orm.relationship(
        'CurrentPropertySheet', cascade='all, delete-orphan',
        innerjoin=True, lazy='joined',
        collection_class=collections.attribute_mapped_collection('name'),
    )

    def __init__(self, item_type, data=None, rid=None):
        if rid is None:
            rid = uuid.uuid4()
        super(Resource, self).__init__(item_type=item_type, rid=rid)
        if data is not None:
            for k, v in data.items():
                self.propsheets[k] = v

    def __getitem__(self, key):
        return self.data[key].propsheet.properties

    def __setitem__(self, key, value):
        current = self.data.get(key, None)
        if current is None:
            self.data[key] = current = CurrentPropertySheet(name=key, rid=self.rid)
        propsheet = PropertySheet(name=key, properties=value, rid=self.rid)
        current.propsheet = propsheet

    def keys(self):
        return self.data.keys()

    def items(self):
        for k in self.keys():
            yield k, self[k]

    def get(self, key, default=None):
        try:
            return self.propsheets[key]
        except KeyError:
            return default

    @property
    def properties(self):
        return self.propsheets['']

    @property
    def propsheets(self):
        return self

    @property
    def uuid(self):
        return self.rid

    @property
    def tid(self):
        tids = (str(self.data[k].propsheet.tid) for k in self.data.keys())
        return ','.join(sorted(set(tids)))

    @property
    def sid(self):
        """
        In some cases there may be more than one sid, but we care about the
        primary one (with '' key)
        """
        return self.data[''].sid

    def used_for(self, item):
        pass


class Blob(Base):
    """ Binary data
    """
    __tablename__ = 'blobs'
    blob_id = Column(UUID, primary_key=True)
    data = Column(types.LargeBinary)


class TransactionRecord(Base):
    __tablename__ = 'transactions'
    order = Column(types.Integer, autoincrement=True, primary_key=True)
    tid = Column(UUID, default=uuid.uuid4, nullable=False, unique=True)
    data = Column(JSON)
    timestamp = Column(
        types.DateTime(timezone=True), nullable=False, server_default=func.now())
    # A server_default is necessary for the notify_ddl overwrite to work
    xid = Column(types.BigInteger, nullable=True, server_default=null())
    __mapper_args__ = {
        'eager_defaults': True,
    }


# User specific stuff
import cryptacular.bcrypt
crypt = cryptacular.bcrypt.BCRYPTPasswordManager()

def hash_password(password):
    return crypt.encode(password)


class User(Base):
    """
    Application's user model.  Use this if you want to store / manage your own user auth
    """
    __tablename__ = 'users'
    user_id = Column(types.Integer, autoincrement=True, primary_key=True)
    name = Column(types.Unicode(60))
    email = Column(types.Unicode(60), unique=True)

    _password = Column('password', types.Unicode(60))

    @property
    def password(self):
        return self._password

    @password.setter
    def password(self, password):
        self._password = hash_password(password)

    def __init__(self, email, password, name):
        self.name = name
        self.email = email
        self.password = password

    @classmethod
    def get_by_username(cls, email):
        return _DBSESSION.query(cls).filter(cls.email == email).first()

    @classmethod
    def check_password(cls, email, password):
        user = cls.get_by_username(email)
        if not user:
            return False
        return crypt.check(user.password, password)
notify_ddl = DDL("""
    ALTER TABLE %(table)s ALTER COLUMN "xid" SET DEFAULT txid_current();
    CREATE OR REPLACE FUNCTION snovault_transaction_notify() RETURNS trigger AS $$
    DECLARE
    BEGIN
        PERFORM pg_notify('snovault.transaction', NEW.xid::TEXT);
        RETURN NEW;
    END;
    $$ LANGUAGE plpgsql;
    CREATE TRIGGER snovault_transactions_insert AFTER INSERT ON %(table)s
    FOR EACH ROW EXECUTE PROCEDURE snovault_transaction_notify();
""")

event.listen(
    TransactionRecord.__table__, 'after_create',
    notify_ddl.execute_if(dialect='postgresql'),
)


@event.listens_for(PropertySheet, 'before_insert')
def set_tid(mapper, connection, target):
    if target.tid is not None:
        return
    txn = transaction.get()
    data = txn._extension
    target.tid = data['tid']


def register(DBSession):
    event.listen(DBSession, 'before_flush', add_transaction_record)
    event.listen(DBSession, 'before_commit', record_transaction_data)
    event.listen(DBSession, 'after_begin', set_transaction_isolation_level)


def add_transaction_record(session, flush_context, instances):
    txn = transaction.get()
    # Set data with txn.setExtendedInfo(name, value)
    data = txn._extension
    record = data.get('_snovault_transaction_record')
    if record is not None:
        if orm.object_session(record) is None:
            # Savepoint rolled back
            session.add(record)
        # Transaction has already been recorded
        return

    tid = data['tid'] = uuid.uuid4()
    record = TransactionRecord(tid=tid)
    data['_snovault_transaction_record'] = record
    session.add(record)


def record_transaction_data(session):
    txn = transaction.get()
    data = txn._extension
    if '_snovault_transaction_record' not in data:
        return

    record = data['_snovault_transaction_record']

    if txn.description:
        data['description'] = txn.description

    if txn.user:
        data['userid'] = txn.user

    record.data = {k: v for k, v in data.items() if not k.startswith('_')}
    session.add(record)


_set_transaction_snapshot = text(
    "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY;"
    "SET TRANSACTION SNAPSHOT :snapshot_id;"
)


def set_transaction_isolation_level(session, sqla_txn, connection):
    ''' Set appropriate transaction isolation level.
    Doomed transactions can be read-only.
    ``transaction.doom()`` must be called before the connection is used.
    Othewise assume it is a write which must be REPEATABLE READ.
    '''
    if connection.engine.url.drivername != 'postgresql':
        return

    txn = transaction.get()
    if not txn.isDoomed():
        # connection.execute("SET TRANSACTION ISOLATION LEVEL REPEATABLE READ;")
        return

    data = txn._extension
    if 'snapshot_id' in data:
        connection.execute(
            _set_transaction_snapshot,
            snapshot_id=data['snapshot_id'])
    else:
        connection.execute("SET TRANSACTION READ ONLY;")
