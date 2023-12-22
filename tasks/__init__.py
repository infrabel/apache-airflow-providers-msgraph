from invoke import Collection

from . import quality
from . import nb
from . import docs
from . import test

ns = Collection()
ns.add_collection(quality, name="qa")
ns.add_collection(nb)
ns.add_collection(docs)
ns.add_collection(test)