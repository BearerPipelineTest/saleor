import json
import uuid
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple, TypedDict, cast

import graphene
from django.conf import settings
from django.http import HttpRequest, HttpResponse
from django.utils import timezone
from graphql import get_operation_ast

from ...core.models import EventDeliveryAttempt
from .. import traced_payload_generator
from . import GQLOperationResult
from .utils import CustomJsonEncoder, JsonTruncText, hide_sensitive_headers


class GQLOperationPayload(TypedDict):
    name: Optional[JsonTruncText]
    operation_type: Optional[str]
    query: Optional[JsonTruncText]
    result: Optional[JsonTruncText]


class RequestPayload(TypedDict):
    id: str
    method: str
    url: str
    time: float
    headers: Dict[str, str]
    contentLength: int


class ResponsePayload(TypedDict):
    headers: Dict[str, str]
    statusCode: int
    reasonPhrase: str
    contentLength: int


class AppPayload(TypedDict):
    id: str
    name: str


class OperationsPayload(TypedDict):
    count: int
    operations: List[GQLOperationPayload]


class ApiCallPayload(TypedDict):
    request: RequestPayload
    response: ResponsePayload
    app: Optional[AppPayload]
    gql_operations: OperationsPayload


TRUNC_PLACEHOLDER = JsonTruncText(truncated=False)
EMPTY_TRUNC = JsonTruncText(truncated=True)


GQL_OPERATION_PLACEHOLDER: GQLOperationPayload = {
    "name": TRUNC_PLACEHOLDER,
    "operation_type": "subscription",
    "query": TRUNC_PLACEHOLDER,
    "result": TRUNC_PLACEHOLDER,
}
GQL_OPERATION_PLACEHOLDER_SIZE = len(
    json.dumps(GQL_OPERATION_PLACEHOLDER, ensure_ascii=True, cls=CustomJsonEncoder)
)


def json_dumps(obj: Any, pretty=False):
    if pretty:
        return json.dumps(obj, indent=2, ensure_ascii=True, cls=CustomJsonEncoder)
    return json.dumps(obj, ensure_ascii=True, cls=CustomJsonEncoder)


def serialize_gql_operation_result(
    operation: GQLOperationResult, bytes_limit: int
) -> Tuple[GQLOperationPayload, int]:
    bytes_limit -= GQL_OPERATION_PLACEHOLDER_SIZE
    if bytes_limit < 0:
        raise ValueError()
    name: Optional[JsonTruncText] = None
    operation_type: Optional[str] = None
    query: Optional[JsonTruncText] = None
    result: Optional[JsonTruncText] = None
    if operation.name:
        name = JsonTruncText.truncate(operation.name, bytes_limit // 3)
        bytes_limit -= cast(JsonTruncText, name).byte_size
    if operation.query:
        query = JsonTruncText.truncate(
            operation.query.document_string, bytes_limit // 2
        )
        bytes_limit -= cast(JsonTruncText, query).byte_size
        if definition := get_operation_ast(
            operation.query.document_ast, operation.name
        ):
            operation_type = definition.operation
    if operation.result:
        result = JsonTruncText.truncate(json_dumps(operation.result), bytes_limit)
        bytes_limit -= cast(JsonTruncText, result).byte_size
    bytes_limit = max(0, bytes_limit)
    return {
        "name": name,
        "operation_type": operation_type,
        "query": query,
        "result": result,
    }, bytes_limit


def serialize_gql_operation_results(
    operations: List[GQLOperationResult], bytes_limit: int
) -> List[GQLOperationPayload]:
    if bytes_limit - len(operations) * GQL_OPERATION_PLACEHOLDER_SIZE < 0:
        raise ValueError()
    payloads: List[GQLOperationPayload] = []
    for i, operation in enumerate(operations):
        payload_limit = bytes_limit // (len(operations) - i)
        payload, left_bytes = serialize_gql_operation_result(operation, payload_limit)
        payloads.append(payload)
        bytes_limit -= payload_limit - left_bytes
    return payloads


@traced_payload_generator
def generate_api_call_payload(
    request: HttpRequest,
    response: HttpResponse,
    gql_operations: List[GQLOperationResult],
    bytes_limit: int,
) -> str:
    request_payload: RequestPayload = {
        "id": str(uuid.uuid4()),
        "method": request.method or "",
        "url": request.build_absolute_uri(request.get_full_path()),
        "time": getattr(request, "request_time", timezone.now()).timestamp(),
        "headers": hide_sensitive_headers(dict(request.headers)),
        "contentLength": int(request.headers.get("Content-Length") or 0),
    }
    response_payload: ResponsePayload = {
        "headers": hide_sensitive_headers(dict(response.headers)),
        "statusCode": response.status_code,
        "reasonPhrase": response.reason_phrase,
        "contentLength": len(response.content),
    }
    app_payload: Optional[AppPayload] = None
    if app := getattr(request, "app", None):
        app_payload = {
            "id": graphene.Node.to_global_id("App", app.id),
            "name": app.name,
        }
    operations_payload: OperationsPayload = {
        "count": len(gql_operations),
        "operations": [],
    }
    payload: ApiCallPayload = {
        "request": request_payload,
        "response": response_payload,
        "app": app_payload,
        "gql_operations": operations_payload,
    }
    base_dump = json_dumps(payload)
    remaining_bytes = bytes_limit - len(base_dump)
    if remaining_bytes < 0:
        raise ValueError(f"Payload too big. Can't truncate to {bytes_limit}")
    try:
        operations_payload["operations"] = serialize_gql_operation_results(
            gql_operations, remaining_bytes
        )
    except ValueError:
        pass
    return json_dumps(payload)


@traced_payload_generator
def generate_truncated_event_delivery_attempt_payload(
    attempt: "EventDeliveryAttempt",
    next_retry: Optional["datetime"],
    bytes_limit: Optional[int] = None,
) -> str:
    if bytes_limit is None:
        bytes_limit = int(settings.OBSERVABILITY_MAX_PAYLOAD_SIZE)
    delivery_data, webhook_data, app_data = {}, {}, {}
    payload = None
    if delivery := attempt.delivery:
        if delivery.payload:
            payload = delivery.payload.payload
        delivery_data = {
            "id": graphene.Node.to_global_id("EventDelivery", delivery.pk),
            "status": delivery.status,
            "type": delivery.event_type,
            "payload": {
                "contentLength": len(payload or ""),
                "body": TRUNC_PLACEHOLDER,
            },
        }
        if webhook := delivery.webhook:
            app_data = {
                "id": graphene.Node.to_global_id("App", webhook.app.pk),
                "name": webhook.app.name,
            }
            webhook_data = {
                "id": graphene.Node.to_global_id("Webhook", webhook.pk),
                "name": webhook.name,
                "targetUrl": webhook.target_url,
            }
    response_body = attempt.response or ""
    request_headers = json.loads(attempt.request_headers or "{}")
    response_headers = json.loads(attempt.response_headers or "{}")
    data = {
        "eventDeliveryAttempt": {
            "id": graphene.Node.to_global_id("EventDeliveryAttempt", attempt.pk),
            "time": attempt.created_at.timestamp(),
            "duration": attempt.duration,
            "status": attempt.status,
            "nextRetry": next_retry.timestamp() if next_retry else None,
        },
        "request": {
            "headers": request_headers,
        },
        "response": {
            "headers": response_headers,
            "contentLength": len(response_body.encode("utf-8")),
            "body": TRUNC_PLACEHOLDER,
        },
        "eventDelivery": delivery_data,
        "webhook": webhook_data,
        "app": app_data,
    }
    initial_dump = json.dumps(data, ensure_ascii=True, cls=CustomJsonEncoder)
    remaining_bytes = bytes_limit - len(initial_dump)
    if remaining_bytes < 0:
        raise ValueError(f"Payload too big. Can't truncate to {bytes_limit}")
    response_body = JsonTruncText.truncate(response_body, remaining_bytes // 2)
    data["response"]["body"] = response_body
    if payload is not None:
        delivery_data["payload"]["body"] = JsonTruncText.truncate(
            payload, remaining_bytes - response_body.byte_size
        )
    return json.dumps(data, ensure_ascii=True, cls=CustomJsonEncoder)
