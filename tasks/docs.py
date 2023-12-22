from invoke import task
from .colors import Color, colorize

DOCS_BUILD_DIR = '_build\docs'
SERVER_PORT = 9000 + 1


@task
def clean(c):
    """Clean documentation build folder"""
    c.run(f"del /f /s /Q {DOCS_BUILD_DIR}")


@task
def build(c):
    """Run sphinx to build documentation."""
    c.run(
        f"sphinx-build -b html -d {DOCS_BUILD_DIR}\\doctrees docs {DOCS_BUILD_DIR}\\html"
    )


@task
def start(c):
    """Start documentation webserver."""
    c.run(
        f"wt python -m http.server --bind localhost --directory {DOCS_BUILD_DIR}\\html {SERVER_PORT}"
    )

    url = f"http://localhost:{SERVER_PORT}"

    print(f"Documentation hosted in background:\n")
    print(f"-> {colorize(url, underline=True)}\n")


@task
def linkcheck(c):
    """Check external links in documentation."""
    c.run(
        f"sphinx-build -b linkcheck -d {DOCS_BUILD_DIR}\\doctrees docs {DOCS_BUILD_DIR}\\linkcheck",
        warn=True,
    )


@task(post=[clean, build, start], default=True)
def rebuild(c):
    """Rebuild documentation and start documentation webserver."""
