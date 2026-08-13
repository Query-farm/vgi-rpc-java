#!/usr/bin/env python3
# © Copyright 2025-2026, Query.Farm LLC - https://query.farm
# SPDX-License-Identifier: Apache-2.0

r"""Regenerate every brand asset in this repo from one master logo.

The master is committed as ``assets/logo-master.png``: the shield mark on
transparency, at the highest resolution we have.  Everything else is derived,
so a new master is a one-command reroll rather than a hand edit in two places
that drift apart:

    uv run --with pillow python scripts/regenerate_logo_assets.py

Pass ``--master PATH`` to cut the assets from a different source, which also
replaces the committed master.

There are two consumers, and only one of them is a file.  ``assets/vgi-logo.png``
is what the README points at through raw.githubusercontent.com, and what the
javadoc task copies into the generated docs alongside its own CSS.  The other is
the landing page served by the library itself, which has no filesystem to read
from at runtime — the mark is inlined there as a ``data:`` URI, and hand-editing
a base64 blob is not something anyone should do twice.
"""

from __future__ import annotations

import argparse
import base64
import io
import re
import shutil
from pathlib import Path

from PIL import Image

_REPO = Path(__file__).resolve().parent.parent
_MASTER = _REPO / "assets" / "logo-master.png"
_README_LOGO = _REPO / "assets" / "vgi-logo.png"

# The landing page renders the mark at 150 CSS px; 300 keeps that 2x without
# making the inlined blob — which is packaged into the jar — any larger than
# it has to be.
_LANDING_PAGES = (_REPO / "vgirpc" / "src" / "main" / "resources" / "farm" / "query" / "vgirpc" / "http" / "landing.html",)
_LANDING_LOGO_WIDTH = 300

# The inlined copy is palettized. The mark is flat artwork, so 256 colours is
# visually indistinguishable from truecolour, and the difference is not
# marginal: 20 KiB against 99 KiB, base64'd and packaged into every jar.
# FASTOCTREE rather than the default median cut because only it keeps alpha.
_LANDING_LOGO_COLORS = 256

# The README displays at 320 CSS px.  600 is the width the sibling ports use
# for the same job, so the fleet serves one size rather than five.
_README_LOGO_WIDTH = 600

# Anchored on the class so the cupola photo further down the page — which is
# Query.Farm's mark, not this project's — is left alone.
_LANDING_LOGO_RE = re.compile(r'(<img class="logo" src="data:image/png;base64,)([A-Za-z0-9+/=]+)(")')


def _scaled_to_width(logo: Image.Image, width: int) -> Image.Image:
    """Resample *logo* to *width*, preserving aspect ratio."""
    height = round(logo.height * width / logo.width)
    return logo.resize((width, height), Image.LANCZOS)


def _cropped_mark(master: Image.Image) -> Image.Image:
    """Return *master* as RGBA, cropped to the mark's bounding box.

    Args:
        master: The logo, on transparency.

    Returns:
        An RGBA image with no dead margin, so every derived size is tight and
        predictable.

    Raises:
        SystemExit: If the master is fully opaque, which means it is the mark on
            a background rather than on transparency — scaling that would bake
            a white slab into every asset.

    """
    image = master.convert("RGBA")
    alpha = image.getchannel("A")
    if alpha.getextrema() == (255, 255):
        raise SystemExit(f"{_MASTER} has no transparency: it is the mark on a background, not a keyed master")
    bbox = image.getbbox()
    return image.crop(bbox) if bbox else image


def _png_bytes(logo: Image.Image, *, colors: int | None = None) -> bytes:
    """Encode *logo* as PNG, palettized to *colors* when given."""
    if colors is not None:
        logo = logo.quantize(colors=colors, method=Image.FASTOCTREE)
    buffer = io.BytesIO()
    logo.save(buffer, format="PNG", optimize=True)
    return buffer.getvalue()


def _rewrite_landing_page(logo: Image.Image, path: Path) -> None:
    """Swap the inlined mark on the landing page at *path*.

    Args:
        logo: The transparent mark.
        path: The HTML page to rewrite in place.

    Raises:
        SystemExit: If the page does not carry exactly one inlined mark, which
            means the markup moved and a silent no-op would ship the old logo.

    """
    html = path.read_text()
    inlined = _png_bytes(_scaled_to_width(logo, _LANDING_LOGO_WIDTH), colors=_LANDING_LOGO_COLORS)
    encoded = base64.b64encode(inlined).decode("ascii")
    rewritten, count = _LANDING_LOGO_RE.subn(lambda m: m.group(1) + encoded + m.group(3), html)
    if count != 1:
        raise SystemExit(f"{path}: expected exactly one inlined logo, found {count}")
    path.write_text(rewritten)
    print(f"  {path.relative_to(_REPO)!s:44} inlined {len(encoded) // 1024:>4} KiB of base64")


def main() -> None:
    """Cut every derived asset from the master and report what was written."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--master",
        type=Path,
        default=None,
        help="Source logo on transparency. Replaces the committed master when given.",
    )
    args = parser.parse_args()

    if args.master is not None:
        shutil.copyfile(args.master, _MASTER)
    if not _MASTER.exists():
        parser.error(f"no master at {_MASTER}; pass --master PATH")

    logo = _cropped_mark(Image.open(_MASTER))
    print(f"master {Image.open(_MASTER).size} -> mark {logo.size}")

    _scaled_to_width(logo, _README_LOGO_WIDTH).save(_README_LOGO)
    print(f"  {_README_LOGO.relative_to(_REPO)!s:44} {Image.open(_README_LOGO).size!s:12} {_README_LOGO.stat().st_size // 1024:>4} KiB")

    for page in _LANDING_PAGES:
        _rewrite_landing_page(logo, page)


if __name__ == "__main__":
    main()
