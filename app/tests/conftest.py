"""Bootstrap legacy import paths for ``app/`` so pytest can collect tests
that import via the historic ``from abcs.*`` / ``from funcs.*`` aliases.

Removed once Wave 2 migrates all imports to absolute ``app.*`` form.
"""

from __future__ import annotations

from app._path_setup import ensure_legacy_import_paths

ensure_legacy_import_paths()
