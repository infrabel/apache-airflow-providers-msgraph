<p align="center"><h1 class="center-title">Airflow Provider Microsoft Graph API</h1></p>

[![PyPI version](https://badge.fury.io/py/msgraph-sdk.svg)](https://pypi.org/project/apache-airflow-providers-msgraph/)
![Python compatibility](https://img.shields.io/badge/python-3.8_|_3.9_|_3.10_|_3.11-blue)
[![Build status](https://github.com/infrabel/apache-airflow-providers-msgraph/actions/workflows/python-build.yml/badge.svg)](https://github.com/infrabel/apache-airflow-providers-msgraph/actions)

Airflow provider package for Microsoft Graph API.

How to develop a Providers package correctly: https://airflow.apache.org/docs/apache-airflow-providers/

Astronomer Providers registry: https://registry.astronomer.io/providers

Making async API calls with Airflow: https://betterprogramming.pub/making-async-api-calls-with-airflow-dynamic-task-mapping-d0cbd3066ebb

This provider makes use of the official Microsoft Graph Python SDK [msgraph-sdk-python](https://github.com/microsoftgraph/msgraph-sdk-python)


## Documentation

### Installing

```python
pip install apache-airflow-providers-msgraph
```

### Configration

![connection.png](https://raw.githubusercontent.com/infrabel/apache-airflow-providers-msgraph/main/docs/images/connection.png)

### Examples

Getting users:

```python
from airflow.providers.microsoft.msgraph.operators.msgraph import MSGraphSDKAsyncOperator

users_task = MSGraphSDKAsyncOperator(
    task_id="users_delta",
    conn_id="msgraph_api",
    url="users",
)
```

Getting users delta:

```python
from airflow.providers.microsoft.msgraph.operators.msgraph import MSGraphSDKAsyncOperator

users_delta_task = MSGraphSDKAsyncOperator(
    task_id="users_delta",
    conn_id="msgraph_api",
    url="users/delta",
)
```

Getting a site from it's relative path and then get pages related to that site:

```python
from airflow.providers.microsoft.msgraph.operators.msgraph import MSGraphSDKAsyncOperator

site_task = MSGraphSDKAsyncOperator(
    task_id="wgive_site",
    conn_id="msgraph_api",
    url="sites/850v1v.sharepoint.com:/sites/wgive",
    result_processor=lambda context, response: response["id"],
)

site_pages_task = MSGraphSDKAsyncOperator(
    task_id="news_site_pages",
    conn_id="msgraph_api",
    expression=(
        "sites/%s/pages"
        % "{{ site_task.output }}"
    ),
)

site_task >> site_pages_task
```