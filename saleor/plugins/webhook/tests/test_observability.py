from typing import Optional
from unittest.mock import patch

import pytest
from kombu import Connection
from kombu.exceptions import ConnectionError as KombuConnectionError
from kombu.exceptions import KombuError

from ....webhook.event_types import WebhookEventAsyncType
from ..observability import (
    CACHE_KEY,
    FullObservabilityEventsBuffer,
    ObservabilityBuffer,
    ObservabilityConnectionError,
    ObservabilityKombuError,
    ObservabilityUnknownError,
    observability_connection,
)

EVENT_TYPE = WebhookEventAsyncType.OBSERVABILITY_API_CALLS


@pytest.fixture
def memory_broker():
    broker_url = "memory://"
    with Connection(broker_url) as conn:
        yield conn
        # Force channel clear
        conn.transport.Channel.queues = {}


def _make_key(x, prefix="key_prefix"):
    return f"{prefix}:1:{x}"


def _fill_buffer(
    buffer: ObservabilityBuffer,
    events_count: int,
    data: Optional[dict] = None,
):
    data = data if data else {"test": "data"}
    for _ in range(events_count):
        buffer.put_event(data)


def test_clear_buffer(memory_broker):
    with ObservabilityBuffer(memory_broker, EVENT_TYPE) as buffer:
        _fill_buffer(buffer, 10)
        assert len(buffer) == 10
        buffer.clear()
        assert len(buffer) == 0


def test_buffer_if_durable(memory_broker):
    with ObservabilityBuffer(memory_broker, EVENT_TYPE) as buffer:
        buffer.put_event({"test": "data"})
        assert len(buffer) == 1
    with ObservabilityBuffer(memory_broker, EVENT_TYPE, batch=1) as buffer:
        assert len(buffer) == 1
        buffer.get_events()
        assert len(buffer) == 0


@patch("saleor.plugins.webhook.observability.cache.make_key", side_effect=_make_key)
def test_observability_events_buffer_repr(_, memory_broker):
    with ObservabilityBuffer(memory_broker, EVENT_TYPE) as buffer:
        assert _make_key(CACHE_KEY + EVENT_TYPE) in repr(buffer)


@patch("saleor.plugins.webhook.observability.cache.make_key")
def test_multiple_observability_buffers_on_the_same_broker(
    mock_make_key, memory_broker
):
    buffer_a_prefix, buffer_a_size = "first_prefix", 5
    buffer_b_prefix, buffer_b_size = "second_prefix", 3
    mock_make_key.side_effect = lambda x: _make_key(x, prefix=buffer_a_prefix)
    with ObservabilityBuffer(memory_broker, EVENT_TYPE) as buffer:
        _fill_buffer(buffer, buffer_a_size)
        assert len(buffer) == buffer_a_size
    mock_make_key.side_effect = lambda x: _make_key(x, prefix=buffer_b_prefix)
    with ObservabilityBuffer(memory_broker, EVENT_TYPE) as buffer:
        _fill_buffer(buffer, buffer_b_size)
        assert len(buffer) == buffer_b_size
    mock_make_key.side_effect = lambda x: _make_key(x, prefix=buffer_a_prefix)
    with ObservabilityBuffer(memory_broker, EVENT_TYPE) as buffer:
        assert len(buffer) == buffer_a_size


@pytest.mark.parametrize("events,batch_size,batches", [(0, 10, 0), (11, 10, 2)])
def test_buffer_size_in_batches(memory_broker, events, batch_size, batches):
    with ObservabilityBuffer(memory_broker, EVENT_TYPE, batch=batch_size) as buffer:
        assert len(buffer) == 0
        _fill_buffer(buffer, events)
        assert buffer.size_in_batches() == batches


def test_buffer_appends_message_id(memory_broker):
    with ObservabilityBuffer(memory_broker, EVENT_TYPE, batch=1) as buffer:
        buffer.put_event({"test": "data"})
        event = buffer.get_events()[0]
        assert buffer.MESSAGE_ID_KEY in event


def test_buffer_does_not_override_message_id(memory_broker):
    with ObservabilityBuffer(memory_broker, EVENT_TYPE, batch=1) as buffer:
        EVENT = {buffer.MESSAGE_ID_KEY: "message-id", "test": "data"}
        buffer.put_event(EVENT)
        event = buffer.get_events()[0]
        assert event == EVENT


def test_buffer_max_length(memory_broker):
    with ObservabilityBuffer(memory_broker, EVENT_TYPE, max_length=10) as buffer:
        _fill_buffer(buffer, 10)
        with pytest.raises(FullObservabilityEventsBuffer):
            buffer.put_event({"skiped": "event"})
        assert len(buffer) == 10


@pytest.mark.parametrize(
    "error,observability_error",
    [
        (KombuConnectionError, ObservabilityConnectionError),
        (KombuError, ObservabilityKombuError),
        (Exception, ObservabilityUnknownError),
    ],
)
@patch("saleor.plugins.webhook.observability.ObservabilityBuffer.put_event")
def test_observability_connection_catch_all_exceptions(
    mock_put, memory_broker, error, observability_error
):
    mock_put.side_effect = error
    with pytest.raises(observability_error):
        with observability_connection(memory_broker) as conn:
            with ObservabilityBuffer(conn, EVENT_TYPE) as buffer:
                buffer.put_event({"test": "data"})
