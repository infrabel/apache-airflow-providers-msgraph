import msgraph
import msgraph_beta
from airflow.providers.microsoft.msgraph.hooks.evaluator import ExpressionEvaluator
from assertpy import assert_that
from mockito import mock, when
from msgraph.generated.users.delta.delta_request_builder import DeltaRequestBuilder
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
from msgraph_beta.generated.models.base_item_collection_response import BaseItemCollectionResponse
from msgraph_beta.generated.sites.item.items.items_request_builder import ItemsRequestBuilder
from msgraph_beta.generated.sites.item.lists.item.list_item_request_builder import ListItemRequestBuilder
from msgraph_beta.generated.sites.item.lists.lists_request_builder import ListsRequestBuilder
from msgraph_beta.generated.sites.item.site_item_request_builder import SiteItemRequestBuilder
from msgraph_beta.generated.sites.sites_request_builder import SitesRequestBuilder

from tests.unit.base import BaseTestCase
from tests.unit.conftest import load_json


class ExpressionEvaluatorTestCase(BaseTestCase):
    @staticmethod
    async def mock_get(value):
        return value

    def test_evaluate_when_expression_is_valid(self):
        users = load_json("resources", "users.json")
        delta_request_builder = mock(spec=DeltaRequestBuilder)
        when(delta_request_builder).get().thenReturn(self.mock_get(users))
        users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
        client = mock({"users": users_request_builder}, spec=msgraph.GraphServiceClient)

        actual = self._loop.run_until_complete(ExpressionEvaluator(client=client).evaluate("users.delta.get()"))

        assert_that(actual).is_equal_to(users)

    def test_evaluate_when_expression_is_invalid(self):
        delta_request_builder = mock(spec=DeltaRequestBuilder)
        users_request_builder = mock({"delta": delta_request_builder}, spec=UsersRequestBuilder)
        client = mock({"users": users_request_builder}, spec=msgraph.GraphServiceClient)

        with self.assertRaises(AttributeError) as context:
            self._loop.run_until_complete(ExpressionEvaluator(client=client).evaluate("users.delta.get()"))

        assert_that(str(context.exception)).is_equal_to("'Dummy' has no attribute 'get' configured")

    def test_evaluate_when_expression_with_parameter_is_valid(self):
        site_id = "accinfrabel.sharepoint.com:/sites/news"
        site = load_json("resources", "site.json")
        site_item_request_builder = mock(spec=SiteItemRequestBuilder)
        when(site_item_request_builder).get().thenReturn(self.mock_get(site))
        sites_request_builder = mock(spec=SitesRequestBuilder)
        when(sites_request_builder).by_site_id(site_id).thenReturn(site_item_request_builder)
        client = mock({"sites": sites_request_builder}, spec=msgraph_beta.GraphServiceClient)

        actual = self._loop.run_until_complete(ExpressionEvaluator(client=client).evaluate(f"sites.by_site_id('{site_id}').get()"))

        assert_that(actual).is_equal_to(site)

    def test_evaluate_when_expression_with_another_valid_parameter(self):
        site_id = "accinfrabel.sharepoint.com,dab36736-0b47-44c1-9543-3688bd792230,1b30fecf-4330-4899-b249-104c2afaf9ed"
        site = load_json("resources", "site.json")
        site_item_request_builder = mock(spec=SiteItemRequestBuilder)
        when(site_item_request_builder).get().thenReturn(self.mock_get(site))
        sites_request_builder = mock(spec=SitesRequestBuilder)
        when(sites_request_builder).by_site_id(site_id).thenReturn(site_item_request_builder)
        client = mock({"sites": sites_request_builder}, spec=msgraph_beta.GraphServiceClient)

        actual = self._loop.run_until_complete(ExpressionEvaluator(client=client).evaluate(f"sites.by_site_id('{site_id}').get()"))

        assert_that(actual).is_equal_to(site)

    def test_get_arguments_with_multiple_quoted_parameters_containg_commas(self):
        site_id = "accinfrabel.sharepoint.com,dab36736-0b47-44c1-9543-3688bd792230,1b30fecf-4330-4899-b249-104c2afaf9ed"
        client = mock(spec=msgraph_beta.GraphServiceClient)
        method_name, args, accessor = ExpressionEvaluator(client=client).get_arguments(f"by_site_id('{site_id}',\"{site_id}\")")

        assert_that(method_name).is_equal_to("by_site_id")
        assert_that(args).is_length(2).contains_only(site_id)
        assert_that(accessor).is_none()

    def test_evaluate_when_expression_with_dict_as_parameter_should_become_dataclass(self):
        site_id = "accinfrabel.sharepoint.com,dab36736-0b47-44c1-9543-3688bd792230,1b30fecf-4330-4899-b249-104c2afaf9ed"
        list_id = "82f9d24d-6891-4790-8b6d-f1b2a1d0ca22"
        item = mock(spec=BaseItemCollectionResponse)
        lists_request_builder = mock(spec=ListsRequestBuilder)
        items = mock(spec=ItemsRequestBuilder)
        request_configuration = ItemsRequestBuilder.ItemsRequestBuilderGetRequestConfiguration(
            query_parameters=ItemsRequestBuilder.ItemsRequestBuilderGetQueryParameters(expand=["fields"])
        )
        when(items).get(request_configuration).thenReturn(self.mock_get([item]))
        lists_item_request_builder = mock({"items": items}, spec=ListItemRequestBuilder)
        when(lists_request_builder).by_list_id(list_id).thenReturn(lists_item_request_builder)
        site_item_request_builder = mock({"lists": lists_request_builder}, spec=SiteItemRequestBuilder)
        sites_request_builder = mock(spec=SitesRequestBuilder)
        when(sites_request_builder).by_site_id(site_id).thenReturn(site_item_request_builder)
        client = mock({"sites": sites_request_builder}, spec=msgraph_beta.GraphServiceClient)

        actual = self._loop.run_until_complete(ExpressionEvaluator(client=client).evaluate(f"sites.by_site_id('{site_id}').lists.by_list_id('{list_id}').items.get({{'query_parameters': {{'expand': ['fields']}}}})"))

        assert_that(actual).contains_only(item)

    def test_evaluate_when_expression_which_has_accessor_on_dict(self):
        site_id = "accinfrabel.sharepoint.com,dab36736-0b47-44c1-9543-3688bd792230,1b30fecf-4330-4899-b249-104c2afaf9ed"
        site = load_json("resources", "site.json")
        site_item_request_builder = mock(spec=SiteItemRequestBuilder)
        when(site_item_request_builder).get().thenReturn(self.mock_get(site))
        sites_request_builder = mock(spec=SitesRequestBuilder)
        when(sites_request_builder).by_site_id(site_id).thenReturn(site_item_request_builder)
        client = mock({"sites": sites_request_builder}, spec=msgraph_beta.GraphServiceClient)

        actual = self._loop.run_until_complete(ExpressionEvaluator(client=client).evaluate(f"sites.by_site_id('{site_id}').get()['displayName']"))

        assert_that(actual).is_equal_to("Communication site")

    def test_evaluate_when_expression_which_has_accessor_on_list(self):
        site = load_json("resources", "site.json")
        sites_request_builder = mock(spec=SitesRequestBuilder)
        when(sites_request_builder).get().thenReturn(self.mock_get([site]))
        client = mock({"sites": sites_request_builder}, spec=msgraph_beta.GraphServiceClient)

        actual = self._loop.run_until_complete(ExpressionEvaluator(client=client).evaluate("sites.get()[-1]"))

        assert_that(actual).is_equal_to(site)
