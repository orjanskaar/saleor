import math
from contextlib import contextmanager
from datetime import datetime, timedelta
from typing import TYPE_CHECKING, Generator, List, Optional, cast
from uuid import uuid4

from django.conf import settings
from django.core.cache import cache
from django.utils import timezone
from kombu import Connection, Exchange, Queue, pools
from kombu.exceptions import ChannelError, KombuError
from kombu.simple import SimpleQueue

from ...plugins.manager import get_plugins_manager
from ...webhook.event_types import WebhookEventAsyncType

if TYPE_CHECKING:
    from celery.exceptions import Retry

    from ...core.models import EventDeliveryAttempt

OBSERVABILITY_EXCHANGE_NAME = "observability_exchange"
CACHE_KEY = "buffer_"
EXCHANGE = Exchange(OBSERVABILITY_EXCHANGE_NAME, type="direct")
CONNECT_TIMEOUT = 0.2
DRAIN_EVENTS_TIMEOUT = 10


class ObservabilityError(Exception):
    """Common subclass for all Observability exceptions."""


class ObservabilityKombuError(ObservabilityError):
    """Observability Kombu error."""


class ObservabilityConnectionError(ObservabilityError):
    """Observability broker connection error."""


class FullObservabilityEventsBuffer(ObservabilityError):
    def __init__(self, event_type: str):
        super().__init__(f"Observability buffer ({event_type}) is full.")
        self.event_type = event_type


class ObservabilityBuffer(SimpleQueue):
    MESSAGE_ID_KEY = "id"
    no_ack = True

    @staticmethod
    def _queue_name(event_type: str) -> str:
        return cache.make_key(CACHE_KEY + event_type)

    @staticmethod
    def _routing_key(event_type: str) -> str:
        return f"{OBSERVABILITY_EXCHANGE_NAME}.{cache.make_key(event_type)}"

    def __init__(
        self,
        channel,
        event_type: str,
        batch: int = 10,
        max_length: int = 100,
    ):
        self.queue_name = self._queue_name(event_type)
        routing_key = self._routing_key(event_type)
        queue = Queue(self.queue_name, EXCHANGE, routing_key=routing_key)
        super().__init__(channel, queue)
        self.event_type = event_type
        self.batch = max(0, batch)
        self.max_length = max(0, max_length)

    def get(self, block=True, timeout=DRAIN_EVENTS_TIMEOUT):
        return super().get(block=block, timeout=timeout)

    def qsize(self):
        try:
            return super().qsize()
        except ChannelError:
            # Let's suppose that queue size is 0 if it not exists
            return 0

    def __repr__(self):
        return f"ObservabilityEventsBuffer('{self.queue_name}')"

    def size_in_batches(self) -> int:
        return math.ceil(self.qsize() / self.batch)

    def put_event(self, json_payload: str):
        if len(self) >= self.max_length:
            raise FullObservabilityEventsBuffer(self.event_type)
        self.put(
            json_payload,
            retry=False,
            timeout=CONNECT_TIMEOUT,
            content_type="application/json",
            compression="zlib",
        )

    def get_events(self):
        self.consumer.qos(prefetch_count=self.batch)
        events: List[dict] = []
        for _ in range(self.batch):
            try:
                message = self.get()
                payload = cast(dict, message.decode())
                if self.MESSAGE_ID_KEY not in payload:
                    if message.delivery_tag:
                        payload[self.MESSAGE_ID_KEY] = message.delivery_tag
                    else:
                        payload[self.MESSAGE_ID_KEY] = str(uuid4())
                events.append(payload)
            except self.Empty:
                break
        return events


def observability_broker() -> Connection:
    return Connection(
        settings.OBSERVABILITY_BROKER_URL, connect_timeout=CONNECT_TIMEOUT
    )


@contextmanager
def observability_connection(
    conn: Optional[Connection] = None,
) -> Generator[Connection, None, None]:
    connection = conn if conn else observability_broker()
    connection_errors = connection.connection_errors + connection.channel_errors
    try:
        connection = pools.connections[connection].acquire(block=False)
        yield connection
    except connection_errors as err:
        raise ObservabilityConnectionError() from err
    except KombuError as err:
        raise ObservabilityKombuError() from err
    finally:
        connection.release()


def task_next_retry_date(retry_error: "Retry") -> Optional[datetime]:
    if isinstance(retry_error.when, (int, float)):
        return timezone.now() + timedelta(seconds=retry_error.when)
    elif isinstance(retry_error.when, datetime):
        return retry_error.when
    return None


def observability_event_delivery_attempt(
    event_type: str,
    attempt: "EventDeliveryAttempt",
    next_retry: Optional[datetime] = None,
):
    if event_type not in WebhookEventAsyncType.OBSERVABILITY_EVENTS:
        get_plugins_manager().observability_event_delivery_attempt(attempt, next_retry)


@contextmanager
def _get_buffer(event_type: str) -> Generator[ObservabilityBuffer, None, None]:
    if event_type not in WebhookEventAsyncType.OBSERVABILITY_EVENTS:
        raise ValueError(f"Unsupported event_type value: {event_type}")
    with observability_connection() as conn:
        with ObservabilityBuffer(
            conn,
            event_type,
            batch=settings.OBSERVABILITY_BUFFER_BATCH,
            max_length=settings.OBSERVABILITY_BUFFER_SIZE_LIMIT,
        ) as buffer:
            yield buffer


def observability_buffer_put_event(event_type: str, event: dict):
    with _get_buffer(event_type) as buffer:
        buffer.put_event(event)


def observability_buffer_get_events(event_type: str) -> List[dict]:
    with _get_buffer(event_type) as buffer:
        return buffer.get_events()


def observability_buffer_size_in_batches(event_type: str) -> int:
    with _get_buffer(event_type) as buffer:
        return buffer.size_in_batches()
