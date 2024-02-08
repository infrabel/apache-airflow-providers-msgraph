from typing import Dict, TypeVar, Type, TYPE_CHECKING

import msgraph
import msgraph_beta
from airflow.utils.module_loading import import_string
from msgraph_core import APIVersion

if TYPE_CHECKING:
    from types import ModuleType

DEFAULT_CONN_NAME = "msgraph_default"
CLIENT_TYPE: TypeVar = TypeVar(
    "CLIENT_TYPE",
    msgraph.GraphServiceClient,  # pylint: disable=E1101
    msgraph_beta.GraphServiceClient,  # pylint: disable=E1101
)
SDK_MODULES: Dict[APIVersion, "ModuleType"] = {
    APIVersion.v1: msgraph,
    APIVersion.beta: msgraph_beta,
}
ODATA_ERROR_TYPE: Dict[APIVersion, Type] = {
    APIVersion.v1: import_string(
        "msgraph.generated.models.o_data_errors.o_data_error.ODataError"
    ),
    APIVersion.beta: import_string(
        "msgraph_beta.generated.models.o_data_errors.o_data_error.ODataError"
    ),
}
