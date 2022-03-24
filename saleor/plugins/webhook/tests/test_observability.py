import json
from unittest.mock import Mock, call, patch

import pytest
from celery.canvas import Signature
from django.conf import settings

from ....webhook.event_types import WebhookEventAsyncType
from ..tasks import observability_reporter_task, observability_send_events


@patch("saleor.plugins.webhook.tasks.send_webhook_using_scheme_method")
@patch("saleor.plugins.webhook.tasks._get_webhooks_for_event")
@patch("saleor.plugins.webhook.tasks.observability_buffer_get_events")
def test_observability_send_events(
    mocked_buffer_get_events,
    mocked_get_webhooks_for_event,
    mocked_send_response,
    any_webhook,
    webhook_response,
):
    event_type = WebhookEventAsyncType.OBSERVABILITY_API_CALLS
    events_data = [{"observability": "event"}]
    mocked_send_response.return_value = webhook_response
    mocked_get_webhooks_for_event.return_value = [any_webhook]
    mocked_buffer_get_events.return_value = events_data

    observability_send_events(event_type)

    mocked_send_response.assert_called_once_with(
        any_webhook.target_url,
        "mirumee.com",
        any_webhook.secret_key,
        event_type,
        json.dumps(events_data),
    )


@pytest.mark.django_db
@pytest.mark.count_queries(autouse=False)
@patch("saleor.plugins.webhook.tasks.observability_buffer_get_events", return_value=[])
def test_observability_send_events_when_empty_buffer(
    _, django_assert_num_queries, count_queries
):
    event_type = WebhookEventAsyncType.OBSERVABILITY_API_CALLS

    with django_assert_num_queries(0):
        observability_send_events(event_type)


@patch("saleor.plugins.webhook.tasks.send_webhook_using_scheme_method")
@patch("saleor.plugins.webhook.tasks._get_webhooks_for_event")
@patch("saleor.plugins.webhook.tasks.observability_buffer_get_events")
def test_observability_send_events_to_external_queue(
    mocked_buffer_get_events,
    mocked_get_webhooks_for_event,
    mocked_send_response,
    any_webhook,
    webhook_response,
    webhook_response_failed,
):
    event_type = WebhookEventAsyncType.OBSERVABILITY_API_CALLS
    target_url = "gcpubsub://cloud.google.com/projects/saleor/topics/test"
    events_data = [{"event": "1"}, {"event": "2"}, {"event": "3"}]
    any_webhook.target_url = target_url
    mocked_send_response.side_effect = [
        webhook_response,
        webhook_response_failed,
        webhook_response,
    ]
    mocked_get_webhooks_for_event.return_value = [any_webhook]
    mocked_buffer_get_events.return_value = events_data

    observability_send_events(event_type)

    calls = [
        call(
            any_webhook.target_url,
            "mirumee.com",
            any_webhook.secret_key,
            event_type,
            json.dumps(events_data[0]),
        ),
        call(
            any_webhook.target_url,
            "mirumee.com",
            any_webhook.secret_key,
            event_type,
            json.dumps(events_data[1]),
        ),
    ]
    mocked_send_response.assert_has_calls(calls)


@patch("saleor.plugins.webhook.tasks.group")
@patch("saleor.plugins.webhook.tasks.observability_buffer_size_in_batches")
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
