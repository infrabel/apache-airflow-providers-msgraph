from distutils.version import StrictVersion

from airflow.providers.microsoft.msgraph.version import (
    __version__,
)

from tests.unit.conftest import VERSION


class TestVersion:
    def test_version(self):
        assert __version__ == VERSION

    def test_version_major_minor_patch(self):
        major, minor, patch = StrictVersion(__version__).version
        expected_major, expected_minor, expected_patch = StrictVersion(VERSION).version

        assert major == expected_major
        assert minor == expected_minor
        assert patch == expected_patch
