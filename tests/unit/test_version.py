from distutils.version import StrictVersion
from unittest import TestCase

from airflow.providers.microsoft.msgraph.version import (
    __version__,
)
from assertpy import assert_that

from tests.unit.conftest import VERSION


class VersionTestCase(TestCase):
    def test_version(self):
        assert_that(__version__).is_equal_to(VERSION)

    def test_version_major_minor_patch(self):
        major, minor, patch = StrictVersion(__version__).version
        expected_major, expected_minor, expected_patch = StrictVersion(VERSION).version

        assert_that(major).is_equal_to(expected_major)
        assert_that(minor).is_equal_to(expected_minor)
        assert_that(patch).is_equal_to(expected_patch)
