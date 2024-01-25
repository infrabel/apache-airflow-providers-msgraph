from airflow.providers.microsoft.msgraph import get_provider_info
from assertpy import assert_that

from tests.unit.base import BaseTestCase
from tests.unit.conftest import VERSION


class ProviderTestCase(BaseTestCase):
    def test_get_provider_info_package_name(self):
        actual = get_provider_info()

        assert_that(actual["package-name"]).is_equal_to("apache-airflow-providers-msgraph")
        assert_that(actual["versions"]).contains_only(VERSION)

