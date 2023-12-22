from unittest import TestCase

from airflow.providers.microsoft.msgraph.version import (
    __version__,
    version_as_dict,
    version_as_tuple,
)
from assertpy import assert_that

from tests.unit.conftest import VERSION


class VersionTestCase(TestCase):
    def test_version(self):
        assert_that(__version__).is_equal_to(VERSION)

    def test_version_as_dict(self):
        version = version_as_dict()
        major, minor, patch = VERSION.split(".")

        assert_that(major).is_equal_to(version.get("major", None))
        assert_that(minor).is_equal_to(version.get("minor", None))
        assert_that(patch).is_equal_to(version.get("patch", None))

    def test_version_as_tuple(self):
        major_, minor_, patch_ = version_as_tuple()
        major, minor, patch = VERSION.split(".")

        assert_that(major).is_equal_to(major_)
        assert_that(minor).is_equal_to(minor_)
        assert_that(patch).is_equal_to(patch_)
