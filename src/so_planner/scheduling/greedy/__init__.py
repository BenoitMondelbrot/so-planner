"""Greedy scheduler modular package.

Note: To avoid circular imports with the legacy monolith
(`greedy_scheduler.py`), this package does not import submodules at
package import time. Import needed modules directly, e.g.:

    from so_planner.scheduling.greedy.loaders import load_plan_of_sales

Other helper modules exist under this package but are intentionally not
eagerly imported here.
"""

__all__: list[str] = []
