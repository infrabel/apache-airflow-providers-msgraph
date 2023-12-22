from invoke import task
from .colors import colorize

BUILD_DIR = "_build/htmlcov"
COV_PORT = 9000 + 2
PROJECT_FOLDER = 'airflow/providers/infrabel/xcom_backend'

@task(help={"verbose": "Run tests verbose."})
def run(c, verbose=False):
    """Run test suite."""
    if verbose:
        c.run(
            f"poetry run pytest -v -W ignore::UserWarning \
            --cov={PROJECT_FOLDER} --cov-report=term:skip-covered --cov-report=html --cov-report=html:{BUILD_DIR}"
        )
    else:
        c.run(
            f"poetry run pytest -W ignore::UserWarning \
            --cov={PROJECT_FOLDER} --cov-report=term:skip-covered --cov-report=html --cov-report=html:{BUILD_DIR}"
        )


@task
def coverage(c):
    """Start coverage report webserver."""
    print(f"python -m http.server --bind localhost --directory {BUILD_DIR} {COV_PORT}")
    c.run(f"wt python -m http.server --bind localhost --directory {BUILD_DIR} {COV_PORT}")

    url = f"http://localhost:{COV_PORT}"

    print(f"Coverage server hosted in background:\n")
    print(f"-> {colorize(url, underline=True)}\n")
    print(f"Stop server: {colorize('inv test.stop')}\n")


@task(post=[run, coverage], default=True)
def all(c):
    """Run all tests and start coverage report webserver."""
