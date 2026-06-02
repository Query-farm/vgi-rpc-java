"""Keep pytest cache out of the vgi-rpc-java tree.

Avoids cluttering the repo with .pytest_cache and keeps cross-run state off
disk so rebuilding is fully deterministic.
"""
import os
import sys

# The conformance suite is re-exported from vgi_rpc; make sure it's importable.
_VENV_SITE = "/Users/rusty/Development/vgi-rpc/.venv/lib/python3.13/site-packages"
if os.path.isdir(_VENV_SITE) and _VENV_SITE not in sys.path:
    sys.path.insert(0, _VENV_SITE)
