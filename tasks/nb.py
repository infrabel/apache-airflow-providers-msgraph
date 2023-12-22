import os
from invoke import task

NB_PORT = 9000


@task(default=True)
def start(c):
    """Start notebook server in background."""
    c.run(f"set JUPYTER_CONFIG_DIR={os.path.dirname(os.getcwd())}\\.jupyter")
    c.run(f"wt jupyter notebook --port={NB_PORT} --no-browser")