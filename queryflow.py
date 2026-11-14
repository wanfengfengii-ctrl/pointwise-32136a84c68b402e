"""Ordered multiset transformation facade. The interpreter was split into
exprkernel (expressions), relationkernel (relations) and coordinator (routing);
this module only keeps the public entry point for existing callers."""
import coordinator


def run(query, tables, params=None):
    return coordinator.run(query, tables, params)
