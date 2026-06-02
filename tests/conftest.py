"""Make the vgi_rpc reference package importable for the conformance driver.

The suite is re-exported from the Python `vgi_rpc` package. Normally that
package is installed in the interpreter running pytest (CI pip-installs it, or
locally you run via the reference venv's python), so nothing extra is needed.

As an escape hatch, set VGI_RPC_SITE to a site-packages directory and it will
be prepended to sys.path — useful when running under an interpreter that
doesn't have vgi_rpc installed but can borrow it from elsewhere.
"""
import os
import sys

_site = os.environ.get("VGI_RPC_SITE")
if _site and os.path.isdir(_site) and _site not in sys.path:
    sys.path.insert(0, _site)
