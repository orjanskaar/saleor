import json
from datetime import datetime
from typing import Optional
from unittest import mock
from unittest.mock import Mock, patch

import pytest
import pytz
from celery.canvas import Signature
from celery.exceptions import Retry
from django.conf import settings
from freezegun import freeze_time
from kombu import Connection
from kombu.exceptions import ConnectionError as KombuConnectionError
from kombu.exceptions import KombuError

from ....webhook.event_types import WebhookEventAsyncType, WebhookEventSyncType
from ..observability import (
    CACHE_KEY,
    FullObservabilityEventsBuffer,
    ObservabilityBuffer,
    ObservabilityConnectionError,
    ObservabilityKombuError,
    _get_buffer,
    observability_connection,
    observability_event_delivery_attempt,
    task_next_retry_date,
)
from ..tasks import observability_reporter_task, observability_send_events

EVENT_TYPE = WebhookEventAsyncType.OBSERVABILITY_API_CALLS
MEMORY_BROKER_URL = "memory://"


@pytest.fixture
def memory_broker():
    with Connection(MEMORY_BROKER_URL) as conn:
        yield conn
        # Force channel clear
        conn.transport.Channel.queues = {}


def _make_key(x, prefix="key_prefix"):
    return f"{prefix}:1:{x}"


def _fill_buffer(
    buffer: ObservabilityBuffer,
    events_count: int,
    data: Optional[str] = None,
):
    data = data or json.dumps({"test": "data"})
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
        buffer.put_event(json.dumps({"test": "data"}))
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


def test_buffer_get_events(memory_broker):
    with ObservabilityBuffer(memory_broker, EVENT_TYPE, batch=20) as buffer:
        _fill_buffer(buffer, 10)
        events = buffer.get_events()

        assert len(events) == 10
        assert len(buffer) == 0


def test_buffer_appends_message_id(memory_broker):
    with ObservabilityBuffer(memory_broker, EVENT_TYPE, batch=1) as buffer:
        buffer.put_event(json.dumps({"test": "data"}))
        event = buffer.get_events()[0]
        assert buffer.MESSAGE_ID_KEY in event


def test_buffer_does_not_override_message_id(memory_broker):
    with ObservabilityBuffer(memory_broker, EVENT_TYPE, batch=1) as buffer:
        EVENT = {buffer.MESSAGE_ID_KEY: "message-id", "test": "data"}
        buffer.put_event(json.dumps(EVENT))
        event = buffer.get_events()[0]
        assert event == EVENT


def test_buffer_max_length(memory_broker):
    with ObservabilityBuffer(memory_broker, EVENT_TYPE, max_length=10) as buffer:
        _fill_buffer(buffer, 10)
        with pytest.raises(FullObservabilityEventsBuffer):
            buffer.put_event(json.dumps({"skiped": "event"}))
        assert len(buffer) == 10


@pytest.mark.parametrize(
    "error,observability_error",
    [
        (KombuConnectionError, ObservabilityConnectionError),
        (KombuError, ObservabilityKombuError),
        (Exception, Exception),
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
                buffer.put_event(json.dumps({"test": "data"}))


@pytest.mark.parametrize(
    "retry, next_retry_date",
    [
        (Retry(), None),
        (Retry(when=60 * 10), datetime(1914, 6, 28, 11, tzinfo=pytz.utc)),
        (Retry(when=datetime(1914, 6, 28, 11)), datetime(1914, 6, 28, 11)),
    ],
)
@freeze_time("1914-06-28 10:50")
def test_task_next_retry_date(retry, next_retry_date):
    assert task_next_retry_date(retry) == next_retry_date


@pytest.mark.parametrize(
    "event_type",
    [
        et
        for et in WebhookEventAsyncType.ALL + WebhookEventSyncType.ALL
        if et not in WebhookEventAsyncType.OBSERVABILITY_EVENTS
    ],
)
@mock.patch(
    "saleor.plugins.manager.PluginsManager.observability_event_delivery_attempt"
)
def test_observability_event_delivery_attempt_fired(
    mock_report_event_delivery_attempt, event_type, event_attempt
):
    observability_event_delivery_attempt(event_type, event_attempt)
    mock_report_event_delivery_attempt.assert_called_once_with(event_attempt, None)


@pytest.mark.parametrize("event_type", WebhookEventAsyncType.OBSERVABILITY_EVENTS)
@mock.patch(
    "saleor.plugins.manager.PluginsManager.observability_event_delivery_attempt"
)
def test_observability_event_delivery_attempt_not_fired(
    mock_report_event_delivery_attempt, event_type, event_attempt
):
    observability_event_delivery_attempt(event_type, event_attempt)
    mock_report_event_delivery_attempt.assert_not_called()


@mock.patch("saleor.plugins.webhook.tasks.send_webhook_using_scheme_method")
@mock.patch("saleor.plugins.webhook.tasks._get_webhooks_for_event")
@mock.patch("saleor.plugins.webhook.tasks.observability_buffer_get_events")
def test_observability_send_events(
    mocked_buffer_get_events,
    mocked_get_webhooks_for_event,
    mocked_send_response,
    any_webhook,
    settings,
    webhook_response,
):
    event_type = WebhookEventAsyncType.OBSERVABILITY_API_CALLS
    events_data = [{"observability": "event"}]
    mocked_send_response.return_value = webhook_response
    mocked_get_webhooks_for_event.return_value = [any_webhook]
    settings.PLUGINS = ["saleor.plugins.webhook.plugin.WebhookPlugin"]
    mocked_buffer_get_events.return_value = events_data

    observability_send_events(event_type)

    mocked_send_response.assert_called_once_with(
        any_webhook.target_url,
        "mirumee.com",
        any_webhook.secret_key,
        event_type,
        json.dumps(events_data),
    )


@mock.patch("saleor.plugins.webhook.tasks.group")
@mock.patch("saleor.plugins.webhook.tasks.observability_buffer_size_in_batches")
def test_observability_reporter_task(
    mocked_buffer_size_in_batches, mocked_celery_group
):
    batches_count = 3
    mocked_buffer_size_in_batches.return_value = batches_count
    mocked_celery_group.return_value = Mock()

    observability_reporter_task()

    mocked_celery_group.assert_called_once()
    tasks = mocked_celery_group.call_args.args[0]
    assert isinstance(tasks, list)
    assert len(tasks) == batches_count * len(WebhookEventAsyncType.OBSERVABILITY_EVENTS)
    assert isinstance(tasks[0], Signature)
    expiration_time = settings.OBSERVABILITY_REPORT_PERIOD.total_seconds()
    mocked_celery_group.return_value.apply_async.assert_called_once_with(
        expires=expiration_time
    )


def test_get_buffer_verify_event_type():
    with pytest.raises(ValueError):
        with _get_buffer("WRONG_EVENT_TYPE"):
            pass


def test_get_buffer_loads_proper_settings(settings):
    settings.OBSERVABILITY_BROKER_URL = MEMORY_BROKER_URL
    settings.OBSERVABILITY_BUFFER_BATCH = 3
    settings.OBSERVABILITY_BUFFER_SIZE_LIMIT = 5

    with _get_buffer(EVENT_TYPE) as buffer:
        assert buffer.batch == 3
        assert buffer.max_length == 5
