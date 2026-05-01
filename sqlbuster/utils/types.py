"""
Type Definitions and Type Checking Utilities Module

This module defines custom type aliases and type checking utilities used in the SQLBuster project,
provides unified type annotation support, enhancing code readability and type safety.
"""

from typing import Any, Callable, Dict, List, Optional, Union

# SQL-related type aliases
SQLString = str
TableName = str
ColumnName = str

# Clause generator type
ClauseGenerator = Callable[
    [Any], List[Any]
]  # Accepts registry and other parameters, returns ClauseVariant list

# Execution result type
ExecutionResult = bool

# Tree node identifier type
NodeID = str


def is_sql_fragment(value: Any) -> bool:
    """
    Check if value is a valid SQL fragment

    Args:
        value: Value to check

    Returns:
        Returns True if string and non-empty, else False
    """
    return isinstance(value, str) and len(value.strip()) > 0


def is_valid_priority(value: Any) -> bool:
    """
    Check if value is a valid priority

    Args:
        value: Value to check

    Returns:
        Returns True if integer, else False
    """
    return isinstance(value, int)
