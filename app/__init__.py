"""
Compatibility package shim.

This repository has both:
- app/backend/app/...   (actual python application package)
- app/db/...            (SQL/schema assets)

In some cloud runtimes the project root is added to PYTHONPATH, which can make
`import app` resolve to `/src/app` first and break imports like `from app import models`.

By explicitly setting package search paths here, `app.*` will resolve to
`app/backend/app/*` first, while still allowing access to root-level `app/*` assets.
"""

import os

_root = os.path.dirname(__file__)
_backend_app = os.path.join(_root, "backend", "app")

# Search backend app package first, then root app folder.
__path__ = [_backend_app, _root]

