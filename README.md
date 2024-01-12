<p align="center"><h1 class="center-title">Airflow Provider Microsoft Graph API</h1></p>

<p align="center">
    <img src="https://img.shields.io/badge/artifactory-1.0.1-brightgreen" alt="Package version">
    <img src="https://img.shields.io/badge/python-3.8_|_3.9_|_3.10_|_3.11-blue" alt="Python compatibility">
</p>

Airflow provider package for Microsoft Graph API.

How to develop a Providers package correctly: https://airflow.apache.org/docs/apache-airflow-providers/
Astronomer Providers registry: https://registry.astronomer.io/providers
Making async API calls with Airflow: https://betterprogramming.pub/making-async-api-calls-with-airflow-dynamic-task-mapping-d0cbd3066ebb


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
    expression="users.get()",
)
```

Getting users delta:

```python
from airflow.providers.microsoft.msgraph.operators.msgraph import MSGraphSDKAsyncOperator

users_delta_task = MSGraphSDKAsyncOperator(
    task_id="users_delta",
    conn_id="msgraph_api",
    expression="users.delta.get()",
)
```

Getting a site from it's relative path and then get pages related to that site:

```python
from airflow.providers.microsoft.msgraph.operators.msgraph import MSGraphSDKAsyncOperator

site_task = MSGraphSDKAsyncOperator(
    task_id="wgive_site",
    conn_id="msgraph_api",
    expression="sites.by_site_id('850v1v.sharepoint.com:/sites/wgive').get()",
)

site_pages_task = MSGraphSDKAsyncOperator(
    task_id="news_site_pages",
    conn_id="msgraph_api",
    expression=(
        "sites.by_site_id('%s').pages.get()"
        % "{{ ti.xcom_pull(task_ids='wgive_site')['id'] }}"
    ),
)

site_task >> site_pages_task
```

Getting users delta paged results and passing those to another DAG which processes them:

```python
import logging

from airflow import DAG
from airflow.decorators import task
from airflow.providers.microsoft.msgraph.operators.msgraph import MSGraphSDKAsyncOperator
from pendulum import datetime


with DAG(
    "process_users_delta",
    start_date=datetime(2022, 12, 20),
    default_args={
        "owner": "dabla",
    },
    schedule_interval=None,
    catchup=False,
) as dag:
    @task(dag=dag)
    def get_response(**context):
        params = context.get("params")
        response = params.get("response")
        return response
        
    get_response()


with DAG(
    "test_msgraph_sdk",
    start_date=datetime(2022, 12, 20),
    default_args={
        "owner": "dabla",
    },
    schedule="@daily",
    catchup=False,
) as dag:
    users_delta_task = MSGraphSDKAsyncOperator(
        task_id="users_delta",
        conn_id="msgraph_api",
        expression="users.delta.get()",
        trigger_dag_id="process_users_delta",
    )

    site_task = MSGraphSDKAsyncOperator(
        task_id="wget_site",
        conn_id="msgraph_api",
        expression="sites.by_site_id('850v1v.sharepoint.com:/sites/wget').get()",
    )

    site_pages_task = MSGraphSDKAsyncOperator(
        task_id="wget_site_pages",
        conn_id="msgraph_api",
        expression=(
            "sites.by_site_id('%s').pages.get()"
            % "{{ ti.xcom_pull(task_ids='wget_site')['id'] }}"
        ),
    )

    @task(dag=dag)
    def show(response):
        logging.info("response: %s", response)


users_delta_task >> site_task >> site_pages_task >> show(site_pages_task.output)
```