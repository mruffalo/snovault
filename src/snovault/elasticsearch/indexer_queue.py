### Class to manage the items for indexing
# First round will use a standard SQS queue from AWS without Elasticache.

import boto3
import json
import math
import structlog
import socket
import time
import datetime
from typing import Any, Dict, List, Tuple

from pyramid.view import view_config
from pyramid.decorator import reify
from .interfaces import INDEXER_QUEUE, INDEXER_QUEUE_MIRROR
from .indexer_utils import get_uuids_for_types

log = structlog.getLogger(__name__)

def includeme(config):
    config.add_route('queue_indexing', '/queue_indexing')
    config.add_route('indexing_status', '/indexing_status')
    env_name = config.registry.settings.get('env.name')
    config.registry[INDEXER_QUEUE] = QueueManager(config.registry)
    config.registry[INDEXER_QUEUE_MIRROR] = None
    config.scan(__name__)


@view_config(route_name='queue_indexing', request_method='POST', permission="index")
def queue_indexing(request) -> Dict[str, Any]:
    """
    Endpoint to queue items for indexing. Takes a POST request with index
    priviliges which should contain either a list of uuids under "uuids" key
    or a list of collections under "collections" key of its body. Can also
    optionally take "strict" boolean and "target_queue" string.
    """
    response = {
        'notification': 'Failure',
        'number_queued': 0,
        'detail': 'Queue functionality has been removed.',
    }
    return response


@view_config(route_name='indexing_status', request_method='GET')
def indexing_status(request) -> Dict[str, str]:
    """
    Endpoint to check what is currently on the queue. Uses GET requests
    """
    response = {
        'status': 'Failure',
        'detail': 'Queue functionality has been removed.'
    }
    return response


class QueueManager:
    """
    Class for handling the queues responsible for coordinating indexing.
    Contains methods to inititalize queues, add both uuids and collections of
    uuids to the queue, and also various helper methods to receive/delete/replace
    messages on the queue.
    Currently the set up uses 4 queues:
    1. Primary queue for items that are directly posted, patched, or added.
    2. Secondary queue for associated items of those in the primary queue.
    3. Deferred queue for items that are outside of the transaction scope
       of any indexing process and need to be tracked separately.
    4. Dead letter queue (dlq) for handling items that have issues processing
       from either the primary or secondary queues.
    """
    def __init__(self, registry, mirror_env=None):
        """
        __init__ will build all three queues needed with the desired settings.
        batch_size parameters conntrol how many messages are batched together
        """
        self.env_name = mirror_env or registry.settings.get('env.name')
        # local development
        if not self.env_name:
            # make sure it's something aws likes
            backup = socket.gethostname()[:80].replace('.','-')
            # last case scenario
            self.env_name = backup or 'fourfront-backup'
        self.queue_name = self.env_name + '-indexer-queue'
        # secondary queue name
        self.second_queue_name = self.env_name + '-secondary-indexer-queue'
        # deferred queue name
        self.defer_queue_name = self.env_name + '-deferred-indexer-queue'
        self.dlq_name = self.queue_name + '-dlq'

    def add_uuids(self, registry, uuids, strict=False, target_queue='primary', telemetry_id=None):
        """
        Takes a list of string uuids queues them up. Also requires a registry,
        which is passed in automatically when using the /queue_indexing route.

        If strict, the uuids will be queued with info instructing associated
        uuids NOT to be queued. If the secondary queue is targeted, strict
        should be true (though this is not enforced).

        Returns a list of queued uuids and a list of any uuids that failed to
        be queued.
        """
        return [], uuids

    def add_collections(self, registry, collections, strict=False, target_queue='primary',
                        telemetry_id=None):
        """
        Takes a list of collection name and queues all uuids for them.
        Also requires a registry, which is passed in automatically when using
        the /queue_indexing route.

        If strict, the uuids will be queued with info instructing associated
        uuids NOT to be queued.

        Returns a list of queued uuids and a list of any uuids that failed to
        be queued.
        """
        uuids = list(get_uuids_for_types(registry, collections))
        return [], uuids

    def get_queue_url(self, queue_name):
        """
        Simple function that returns url of associated queue name
        """
        return ''

    def get_queue_arn(self, queue_url):
        """
        Get the ARN of the specified queue
        """
        return ''

    def clear_queue(self):
        pass

    def delete_queue(self, queue_url):
        pass

    def chunk_messages(self, messages, chunksize):
        """
        Chunk a given number of messages into chunks of given chunksize
        """
        for i in range(0, len(messages), chunksize):
            yield messages[i:i + chunksize]

    def choose_queue_url(self, name):
        """
        Simple utility function given a string name parameter. Used to select
        between primary, secondary, and deferred queues
        """
        return ''

    def send_messages(self, items: List[Dict[str, Any]], target_queue='primary', retries=0) -> List[str]:
        """
        Send any number of 'items' as messages to sqs.
        items is a list of dictionaries with the following format:
        {
            'uuid': string uuid,
            'sid': int sid from postgres or None for secondary items,
            'strict': boolean that controls if assciated uuids are found,
            'timestamp': datetime string, should be utc,
            'detail': string containing extra information, not always used
        }
        Can batch up to 10 messages, controlled by self.send_batch_size.

        strict is a boolean that determines whether or not associated uuids
        will be found for these uuids.

        Since sending messages is something we want to be fail-proof, retry
        failed messages automatically up to 4 times.
        Returns information on messages that failed to queue despite the retries
        """
        return ['Sending messages is disabled.']

    def receive_messages(self, target_queue='primary') -> List:
        return []

    def delete_messages(self, messages, target_queue='primary'):
        return ['Deleting messages is disabled.']

    def replace_messages(self, messages, target_queue='primary', vis_timeout=5):
        return ['Replacing messages is disabled.']

    def number_of_messages(self) -> Dict[str, int]:
        """
        Returns a dict with number of waiting messages in the queue and
        number of inflight (i.e. not currently visible) messages.
        Also returns info on items in the dlq.
        """
        formatted = {
            'primary_waiting': 0,
            'primary_inflight': 0,
            'secondary_waiting': 0,
            'secondary_inflight': 0,
            'deferred_waiting': 0,
            'deferred_inflight': 0,
            'dlq_waiting': 0,
            'dlq_inflight': 0,
        }
        return formatted
