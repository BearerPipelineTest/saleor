from typing import Callable, Dict, Optional

import pytest
from graphql import get_default_backend

from ....graphql.api import schema
from .. import GQLOperationResult

backend = get_default_backend()
GqlOperationFactoryType = Callable[
    [str, Optional[str], Optional[Dict], Optional[Dict]], GQLOperationResult
]


@pytest.fixture
def gql_operation_factory() -> GqlOperationFactoryType:
    def factory(
        query_string: str,
        operation_name: Optional[str] = None,
        variables: Optional[Dict] = None,
        result: Optional[Dict] = None,
    ) -> GQLOperationResult:
        query = backend.document_from_string(schema, query_string)
        return GQLOperationResult(
            name=operation_name, query=query, variables=variables, result=result
        )

    return factory
