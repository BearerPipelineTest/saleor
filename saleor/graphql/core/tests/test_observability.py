from unittest.mock import patch

EXAMPLE_QUERY = "{ shop { name } }"


@patch("saleor.webhook.observability.reporter.observability_api_call")
def test_observability_report_api_call_with_report_all_api_calls(
    mock_observability_api_call, api_client
):
    query_shop = "{ shop { name } }"

    api_client.post_graphql(query_shop, variables={})

    mock_observability_api_call.assert_called_once()
