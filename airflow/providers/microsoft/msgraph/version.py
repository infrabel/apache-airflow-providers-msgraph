# -*- coding: utf-8 -*-
"""
This module exposes the current version of the package
so the version can be retreived directly within other code
files. The version assumes the semver notation (major.minor.patch).
It's kept in sync with the `bumpversion` tool.

Example:
    Import the version as constant, dict or tuple:

    >>> from airflow.providers.keeper.secrets.version import version_as_dict
    >>> from airflow.providers.keeper.secrets.version import version_as_tuple
    >>> from airflow.providers.keeper.secrets.version import __version__

Attributes:
    __version__ (str): Current version of airflow.providers.infrabel.openshift package.

"""
from typing import Dict, Tuple

__version__: str = "1.0.0"


def version_as_dict() -> Dict[str, str]:
    """Current version as dictionary.

    Splits the semver version (e.g. 1.2.3) into a dictionary
    with major, minor & patch key.
    """
    major, minor, patch = __version__.split(".")
    return {"major": major, "minor": minor, "patch": patch}


def version_as_tuple() -> Tuple[str, str, str]:
    """Current version as tuple.

    Splits the semver version (e.g. 1.2.3) into a tuple
    (major, minor, patch).
    """
    major, minor, patch = __version__.split(".")
    return major, minor, patch
