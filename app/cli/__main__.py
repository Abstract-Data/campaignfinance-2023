from __future__ import annotations

from app._path_setup import ensure_legacy_import_paths

ensure_legacy_import_paths()

from app.cli.main import app  # noqa: E402

if __name__ == "__main__":
    app()
