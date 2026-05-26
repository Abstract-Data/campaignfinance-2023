"""Conftest for tests/states/.

Some legacy Oklahoma state modules (oklahoma_database.py, oklahoma.py)
import production-only packages (op SDK, snowflake.snowpark) at module
level and also run ``create_connection()`` on import.

Strategy: pre-register ``app.states.oklahoma`` as a lightweight stub
package *before* collection, so Python skips the real ``__init__.py``
when resolving ``app.states.oklahoma.validators.*`` imports.  The real
``validators`` sub-package is still importable because its own
``__init__.py`` has no problematic dependencies.
"""

from __future__ import annotations

import importlib
import importlib.util
import sys
import types


def _make_stub(name: str) -> types.ModuleType:
    mod = types.ModuleType(name)
    mod.__path__ = []  # type: ignore[attr-defined]  # mark as package
    mod.__package__ = name
    return mod


# ------------------------------------------------------------------
# 1. Stub out the oklahoma package so its __init__ never executes.
#    Sub-packages/modules can still be imported normally afterwards.
# ------------------------------------------------------------------
if "app.states.oklahoma" not in sys.modules:
    # Ensure parent chain is loaded first
    for _parent in ("app", "app.states"):
        if _parent not in sys.modules:
            try:
                importlib.import_module(_parent)
            except ImportError:
                sys.modules[_parent] = _make_stub(_parent)

    import os as _os

    _ok_real_dir = _os.path.join(
        _os.path.dirname(__file__), "..", "..", "app", "states", "oklahoma"
    )
    _ok_stub = _make_stub("app.states.oklahoma")
    _ok_stub.__path__ = [_os.path.normpath(_ok_real_dir)]  # type: ignore[assignment]
    _ok_stub.__file__ = _os.path.join(_os.path.normpath(_ok_real_dir), "__init__.py")
    sys.modules["app.states.oklahoma"] = _ok_stub

# ------------------------------------------------------------------
# 2. Stub out production-only third-party packages in case anything
#    else in the validators chain tries to reach them.
# ------------------------------------------------------------------
if "op" not in sys.modules:
    _op = _make_stub("op")

    class _OnePasswordItem:
        def __init__(self, *args: object, **kwargs: object) -> None:
            self.database_params: dict = {}

    _op.OnePasswordItem = _OnePasswordItem  # type: ignore[attr-defined]
    sys.modules["op"] = _op

for _sf_mod in ("snowflake", "snowflake.snowpark"):
    if _sf_mod not in sys.modules:
        _sf = _make_stub(_sf_mod)
        if _sf_mod == "snowflake.snowpark":
            _sf.Session = type(  # type: ignore[attr-defined]
                "Session",
                (),
                {
                    "builder": type(
                        "Builder", (), {"configs": staticmethod(lambda *a, **kw: None)}
                    )()
                },
            )
        sys.modules[_sf_mod] = _sf
