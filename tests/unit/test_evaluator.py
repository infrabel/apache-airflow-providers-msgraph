import msgraph
import msgraph_beta
from airflow.providers.microsoft.msgraph.hooks.evaluator import ExpressionEvaluator
from assertpy import assert_that
from mockito import mock, when
from msgraph.generated.users.delta.delta_request_builder import DeltaRequestBuilder
from msgraph.generated.users.users_request_builder import UsersRequestBuilder
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
        method_name, args = ExpressionEvaluator(client=client).get_arguments(f"by_site_id('{site_id}',\"{site_id}\")")

        assert_that(method_name).is_equal_to("by_site_id")
        assert_that(args).is_length(2).contains_only(site_id)
