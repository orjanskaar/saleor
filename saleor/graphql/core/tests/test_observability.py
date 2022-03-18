from unittest.mock import Mock, patch

from django.test import override_settings


@override_settings(OBSERVABILITY_ACTIVE=True)
@override_settings(OBSERVABILITY_REPORT_ALL_API_CALLS=True)
@patch("saleor.core.middleware.get_plugins_manager")
def test_observability_report_api_call(
    mock_get_plugins_manager, api_client, site_settings
):
    plugin_manager = Mock()
    mock_get_plugins_manager.return_value = plugin_manager
    query_shop = "{ shop { name } }"
    api_client.post_graphql(query_shop, variables={})
    plugin_manager.observability_api_call.assert_called_once()
