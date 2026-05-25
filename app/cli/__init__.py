"""Campaign finance CLI (`cf`)."""

from app._path_setup import ensure_legacy_import_paths

ensure_legacy_import_paths()

from app.cli.convert import run_convert  # noqa: E402
from app.cli.download import run_download  # noqa: E402
from app.cli.prepare import run_prepare  # noqa: E402
from app.cli.verify import run_verify  # noqa: E402

__all__ = [
    "run_convert",
    "run_download",
    "run_prepare",
    "run_verify",
]
