from dataclasses import dataclass, field
from typing import Dict, List, Optional

from django.http import HttpRequest, HttpResponse
from graphql import GraphQLDocument


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


@dataclass
class GQLOperationResult:
    name: Optional[str] = None
    query: Optional[GraphQLDocument] = None
    variables: Optional[Dict] = None
    result: Optional[Dict] = None


@dataclass
class ApiCallResponse:
    request: Optional[HttpRequest] = None
    response: Optional[HttpResponse] = None
    queries: List[GQLOperationResult] = field(default_factory=list)
