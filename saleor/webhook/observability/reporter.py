import functools
from contextlib import contextmanager
from contextvars import ContextVar
from typing import Any, Callable, Dict, Generator, Optional, Tuple

from django.http import HttpRequest, HttpResponse
from graphql import GraphQLDocument

from . import ApiCallResponse, GQLOperationResult
from .buffer import observability_api_call

_API_CALL_CONTEXT: ContextVar[Optional[ApiCallResponse]] = ContextVar(
    "api_call_context", default=None
)


_GQL_OPERATION_CONTEXT: ContextVar[Optional[GQLOperationResult]] = ContextVar(
    "gql_operation_context", default=None
)

import opentracing


@contextmanager
def api_call_context() -> Generator[ApiCallResponse, None, None]:
    with opentracing.global_tracer().start_active_span("observability") as scope:
        span = scope.span
        span.set_tag(opentracing.tags.COMPONENT, "observability")
        context = _API_CALL_CONTEXT.get()
        if context is None:
            context = ApiCallResponse()
            _API_CALL_CONTEXT.set(context)
        yield context
        _API_CALL_CONTEXT.set(None)
        observability_api_call(context)


@contextmanager
def gql_operation_context() -> Generator[GQLOperationResult, None, None]:
    context = _GQL_OPERATION_CONTEXT.get()
    if context is None:
        context = GQLOperationResult()
        _GQL_OPERATION_CONTEXT.set(context)
    yield context
    _GQL_OPERATION_CONTEXT.set(None)
    if api_call_context := _API_CALL_CONTEXT.get():
        api_call_context.queries.append(context)


def set_gql_operation(
    *,
    query: Optional[GraphQLDocument] = None,
    name: Optional[Any] = None,
    variables: Optional[Any] = None,
    result: Optional[Dict] = None,
):
    if operation := _GQL_OPERATION_CONTEXT.get():
        if query:
            operation.query = query
        if isinstance(variables, dict):
            operation.variables = variables
        if isinstance(name, str):
            operation.name = name
        if result is not None:
            operation.result = result


def api_call_dispatch_wrapper(dispatch: Callable[..., HttpResponse]):
    @functools.wraps(dispatch)
    def wrapper(self, request: HttpRequest, *args, **kwargs):
        with api_call_context() as context:
            response = dispatch(self, request, *args, **kwargs)
            context.request = request
            context.response = response
        return response

    return wrapper


def api_call_get_response_wrapper(func: Callable[..., Tuple[Optional[Dict], int]]):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        with gql_operation_context() as context:
            result, status_code = func(*args, **kwargs)
            context.result = result
        return result, status_code

    return wrapper
