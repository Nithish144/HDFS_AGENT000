"""
Goal Comparator — Compares current state against goal state.
"""
import logging
from typing import Any

logger = logging.getLogger(__name__)


class GoalComparator:
    def __init__(self, goal_state: dict):
        self.goal_state = goal_state

    def find_gaps(self, current_state: dict) -> list[dict]:
        gaps = []
        for key, expected in self.goal_state.items():
            actual = current_state.get(key)
            if not self._satisfies(actual, expected):
                gaps.append({"field": key, "expected": expected, "actual": actual})
                logger.debug(f"GAP: {key} | expected={expected} | actual={actual}")
        return gaps

    def _satisfies(self, actual: Any, expected: Any) -> bool:
        if expected is None:
            return True
        if isinstance(expected, bool):
            return actual == expected
        if isinstance(expected, int):
            if actual is None:
                return False
            return int(actual) >= expected
        if isinstance(expected, str):
            if actual is None:
                return False
            return str(actual).startswith(str(expected))
        return actual == expected
