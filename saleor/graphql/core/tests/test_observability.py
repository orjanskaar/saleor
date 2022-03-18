from unittest.mock import Mock, patch

from django.test import override_settings

EXAMPLE_QUERY = "{ shop { name } }"


@override_settings(OBSERVABILITY_ACTIVE=False)
@patch("saleor.core.middleware.get_plugins_manager")
def test_observability_report_api_call_not_fired(
    mock_get_plugins_manager, api_client, site_settings
):
    plugin_manager = Mock()
    mock_get_plugins_manager.return_value = plugin_manager

    api_client.post_graphql(EXAMPLE_QUERY, variables={})

    plugin_manager.observability_api_call.assert_not_called()


@override_settings(OBSERVABILITY_ACTIVE=True)
@override_settings(OBSERVABILITY_REPORT_ALL_API_CALLS=False)
@patch("saleor.core.middleware.get_plugins_manager")
def test_observability_report_api_call_on_app_request(
    mock_get_plugins_manager, app_api_client, site_settings
):
    plugin_manager = Mock()
    mock_get_plugins_manager.return_value = plugin_manager
    query_shop = "{ shop { name } }"

    app_api_client.post_graphql(query_shop, variables={})

    plugin_manager.observability_api_call.assert_called_once()


@override_settings(OBSERVABILITY_ACTIVE=True)
@override_settings(OBSERVABILITY_REPORT_ALL_API_CALLS=True)
@patch("saleor.core.middleware.get_plugins_manager")
def test_observability_report_api_call_with_report_all_api_calls(
    mock_get_plugins_manager, api_client, site_settings
):
    plugin_manager = Mock()
    mock_get_plugins_manager.return_value = plugin_manager
    query_shop = "{ shop { name } }"

    api_client.post_graphql(query_shop, variables={})

    plugin_manager.observability_api_call.assert_called_once()
