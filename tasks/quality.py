from invoke import task
from .colors import Color, colorize

PROJECT_FOLDER = 'airflow/providers/infrabel/xcom_backend'

@task
def black(c):
    """Run code formatter: black."""
    print("-> Running black...")
    c.run(f"poetry run black {PROJECT_FOLDER}")


@task
def flake(c):
    """Run style guide enforcement: flake8."""
    print("-> Running flake8...")
    c.run(f"poetry run flake8 --ignore=E501 {PROJECT_FOLDER}", warn=True)


@task
def pylint(c):
    """Run code analysis: pylint."""
    print("-> Running pylint...")
    c.run(f"poetry run pylint {PROJECT_FOLDER}", warn=True)


@task
def mypy(c):
    """Run static type checking: mypy."""
    print("-> Running mypy...")
    c.run(f"poetry run mypy {PROJECT_FOLDER}", warn=True)


@task(post=[black, flake, pylint, mypy], default=True)
def all(c):
    """Run all quality checks."""