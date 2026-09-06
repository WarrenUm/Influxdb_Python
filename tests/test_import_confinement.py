"""Static import-scan enforcing v2-client confinement.

Feature: influxdb-v3-migration

After the cutover to InfluxDB 3, the InfluxDB v2 client (the ``influxdb_client``
package, imported as ``import influxdb_client`` or ``from influxdb_client import
...``) must survive in exactly one place in the runtime package: the migration
tool ``ge_pipeline/migrate.py``. Every other module under ``ge_pipeline/`` must
be free of the v2 client (Requirement 10.4 / design "Cutover check").

This test walks every ``.py`` module under ``ge_pipeline/`` and parses it with
the :mod:`ast` module (rather than substring-grepping the source, which would
false-positive on docstrings, comments, and the v3 ``influxdb_client_3``
package). It inspects the ``import`` / ``from ... import`` statements and fails
if the v2 ``influxdb_client`` top-level package is imported anywhere other than
``migrate.py``.

The v3 client ``influxdb_client_3`` is a *different* top-level package and is
allowed everywhere -- only the exact top-level name ``influxdb_client`` is
confined.
"""

from __future__ import annotations

import ast
from pathlib import Path

# The runtime package whose modules are scanned.
PACKAGE_DIR = Path(__file__).resolve().parent.parent / "ge_pipeline"

# The sole module permitted to import the v2 client.
ALLOWED_MODULE = "migrate.py"

# The exact top-level package name of the InfluxDB v2 client. The v3 client is
# ``influxdb_client_3`` -- a distinct top-level name that must NOT match.
V2_CLIENT_ROOT = "influxdb_client"


def _root_package(dotted_name: str) -> str:
    """Return the top-level package of a dotted module name.

    ``"influxdb_client.client.flux_table"`` -> ``"influxdb_client"`` and
    ``"influxdb_client_3"`` -> ``"influxdb_client_3"`` (which is a different
    package and therefore never matches the v2 root).
    """

    return dotted_name.split(".", 1)[0]


def _imports_v2_client(tree: ast.AST) -> list[str]:
    """Return descriptions of any v2-client imports found in an AST.

    Only ``import influxdb_client[...]`` and ``from influxdb_client[...] import
    ...`` are matched. ``influxdb_client_3`` (the v3 client) is explicitly not a
    match because its top-level package name differs.
    """

    offenders: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            offenders.extend(
                f"line {node.lineno}: import {alias.name}"
                for alias in node.names
                if _root_package(alias.name) == V2_CLIENT_ROOT
            )
        elif (
            isinstance(node, ast.ImportFrom)
            # Relative imports (``from . import x``) have module=None and
            # level>0; those can never reference the v2 client.
            and node.level == 0
            and node.module is not None
            and _root_package(node.module) == V2_CLIENT_ROOT
        ):
            names = ", ".join(alias.name for alias in node.names)
            offenders.append(
                f"line {node.lineno}: from {node.module} import {names}"
            )
    return offenders


def test_v2_client_confined_to_migrate_module() -> None:
    """The v2 ``influxdb_client`` is imported only inside ``migrate.py``.

    Validates: Requirements 10.4
    """

    assert PACKAGE_DIR.is_dir(), f"runtime package not found: {PACKAGE_DIR}"

    leaks: dict[str, list[str]] = {}
    scanned = 0
    for module_path in sorted(PACKAGE_DIR.rglob("*.py")):
        scanned += 1
        source = module_path.read_text(encoding="utf-8")
        tree = ast.parse(source, filename=str(module_path))
        offenders = _imports_v2_client(tree)
        if not offenders:
            continue
        # migrate.py is the one place the v2 client is allowed to live.
        if module_path.name == ALLOWED_MODULE:
            continue
        rel = module_path.relative_to(PACKAGE_DIR.parent)
        leaks[str(rel)] = offenders

    # Sanity: we actually scanned the package (guards against a broken path).
    assert scanned > 0, f"no .py modules scanned under {PACKAGE_DIR}"

    if leaks:
        details = "\n".join(
            f"  {module}:\n" + "\n".join(f"    {line}" for line in offenders)
            for module, offenders in sorted(leaks.items())
        )
        raise AssertionError(
            "The InfluxDB v2 client `influxdb_client` must be imported ONLY in "
            f"ge_pipeline/{ALLOWED_MODULE}, but it leaked into "
            f"{len(leaks)} other module(s):\n{details}"
        )
