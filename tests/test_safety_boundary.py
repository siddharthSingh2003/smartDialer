import ast
import pathlib

FORBIDDEN_ROOTS = {"allocator", "providers", "repo"}

PACING_DIR = pathlib.Path(__file__).resolve().parents[1] / "src" / "smartdialer" / "pacing"


def _imported_roots(tree: ast.AST) -> set[str]:
    roots: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                roots.add(alias.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                roots.add(node.module.split(".")[0])
            if node.level and node.level >= 1:
                # relative import: `from . import X` / `from .. import X`
                if node.module is None and node.names:
                    for alias in node.names:
                        roots.add(alias.name)
    return roots


def test_pacing_engine_cannot_import_allocator_providers_or_repo():
    """Invariant I9: the pacing engine physically cannot place a call. This is
    the third, decisive layer of the non-bypassability proof in
    ARCHITECTURE.md §8 / §11.3 — type-level and wiring-level guarantees are
    only as good as this test staying green."""
    files = list(PACING_DIR.glob("*.py"))
    assert files, "expected to find pacing/*.py"

    offenders: dict[str, set[str]] = {}
    for path in files:
        tree = ast.parse(path.read_text(), filename=str(path))
        roots = _imported_roots(tree)
        hit = roots & FORBIDDEN_ROOTS
        if hit:
            offenders[path.name] = hit

    assert not offenders, f"pacing/*.py must never import {FORBIDDEN_ROOTS}: {offenders}"
