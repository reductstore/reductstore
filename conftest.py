"""Pytest compatibility helpers for workspace-wide test discovery.

Pytest 9.x removed the ``Package.obj`` attribute, but ``pytest-asyncio``
(<=0.23) still expects it to exist and crashes while VS Code tries to discover
our integration tests.  We define a benign fallback so the plugin can gracefully
skip package-level manipulation without raising ``AttributeError``.
"""

from __future__ import annotations

try:
    from _pytest.python import Package
except Exception:  # pragma: no cover - defensive guard for exotic environments
    Package = None  # type: ignore[assignment]

if Package is not None and not hasattr(Package, "obj"):
    # ``pytest-asyncio`` just needs the attribute to exist; ``None`` makes it
    # bail out of the optional package-level logic without breaking collection.
    setattr(Package, "obj", None)
