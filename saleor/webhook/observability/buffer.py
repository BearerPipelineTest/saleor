import logging
import math
from contextlib import contextmanager
from datetime import datetime
from typing import TYPE_CHECKING, Generator, List, Optional, cast

import opentracing
from django.conf import settings
from django.core.cache import cache
from kombu import Connection, Exchange, Queue, pools
from kombu.exceptions import ChannelError, KombuError
from kombu.simple import SimpleQueue

from ...webhook.event_types import WebhookEventAsyncType
from . import (
    ApiCallResponse,
    FullObservabilityEventsBuffer,
    ObservabilityConnectionError,
    ObservabilityError,
    ObservabilityKombuError,
)
from .payloads import (
    generate_api_call_payload,
    generate_truncated_event_delivery_attempt_payload,
)
from .utils import _get_webhooks_for_event

if TYPE_CHECKING:

    from ...core.models import EventDeliveryAttempt

logger = logging.getLogger(__name__)

OBSERVABILITY_EXCHANGE_NAME = "observability_exchange"
CACHE_KEY = "buffer_"
EXCHANGE = Exchange(OBSERVABILITY_EXCHANGE_NAME, type="direct")
CONNECT_TIMEOUT = 0.2
DRAIN_EVENTS_TIMEOUT = 10.0


class ObservabilityBuffer(SimpleQueue):
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

    def get_events(self, timeout=DRAIN_EVENTS_TIMEOUT) -> List[dict]:
        self.consumer.qos(prefetch_count=self.batch)
        events: List[dict] = []
        for _ in range(self.batch):
            try:
                message = self.get(timeout=timeout)
                events.append(cast(dict, message.decode()))
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


def observability_event_delivery_attempt(
    event_type: str,
    attempt: "EventDeliveryAttempt",
    next_retry: Optional[datetime] = None,
):
    with opentracing.global_tracer().start_active_span(
        "observability_event_delivery_attempt"
    ) as scope:
        span = scope.span
        span.set_tag(opentracing.tags.COMPONENT, "observability")
        if event_type in WebhookEventAsyncType.OBSERVABILITY_EVENTS:
            return
        observability_event_type = (
            WebhookEventAsyncType.OBSERVABILITY_EVENT_DELIVERY_ATTEMPTS
        )
        if _get_webhooks_for_event(observability_event_type).exists():
            try:
                event = generate_truncated_event_delivery_attempt_payload(
                    attempt, next_retry
                )
                observability_buffer_put_event(event_type, event)
            except (ValueError, ObservabilityError):
                logger.info("Observability %s event skiped", event_type, exc_info=True)
            except Exception:
                logger.warn("Observability %s event skiped", event_type, exc_info=True)


def observability_api_call(api_call: ApiCallResponse):
    with opentracing.global_tracer().start_active_span(
        "observability_api_call"
    ) as scope:
        span = scope.span
        span.set_tag(opentracing.tags.COMPONENT, "observability")
        if not settings.OBSERVABILITY_ACTIVE:
            return
        if api_call.request is None or api_call.response is None:
            # TODO logging
            return
        if not settings.OBSERVABILITY_REPORT_ALL_API_CALLS and getattr(
            api_call.request, "app", None
        ):
            return
        event_type = WebhookEventAsyncType.OBSERVABILITY_API_CALLS
        with opentracing.global_tracer().start_active_span("get_webhooks") as scope:
            webhooks_exists = _get_webhooks_for_event(event_type).exists()
        if webhooks_exists:
            try:
                event = generate_api_call_payload(
                    api_call.request,
                    api_call.response,
                    api_call.queries,
                    settings.OBSERVABILITY_MAX_PAYLOAD_SIZE,
                )
                observability_buffer_put_event(event_type, event)
            except (ValueError, ObservabilityError):
                logger.info("Observability %s event skiped", event_type, exc_info=True)
            except Exception:
                logger.warn("Observability %s event skiped", event_type, exc_info=True)


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


def observability_buffer_put_event(event_type: str, json_payload: str):
    with opentracing.global_tracer().start_active_span(
        "observability_buffer_put"
    ) as scope:
        span = scope.span
        span.set_tag(opentracing.tags.COMPONENT, "observability")
        with _get_buffer(event_type) as buffer:
            buffer.put_event(json_payload)


def observability_buffer_get_events(
    event_type: str, timeout=DRAIN_EVENTS_TIMEOUT
) -> List[dict]:
    with _get_buffer(event_type) as buffer:
        return buffer.get_events(timeout=timeout)


def observability_buffer_size_in_batches(event_type: str) -> int:
    with _get_buffer(event_type) as buffer:
        return buffer.size_in_batches()
