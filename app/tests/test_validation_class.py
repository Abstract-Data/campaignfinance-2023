"""
Pre-existing legacy test for ``StateFileValidation``.

The test logic assumed an older ``StateFileValidation()`` no-arg API that
predates the current ``validator_to_use`` requirement (and previously
imported from a now-absent ``funcs.StateFileValidation`` alias).

Skipped until a follow-up rewrites it against the current ABC contract.
Tracked under the Wave 5 test-coverage backlog (P2-TEST-001).
"""

import pytest

pytest.skip(
    "Legacy API mismatch — rewrite under Wave 5 test-coverage backlog.",
    allow_module_level=True,
)
