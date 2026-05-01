"""
SQLBuster - A tool for exploring and testing database SQL capabilities

This package provides automated SQL exploration using DFS algorithm
to discover database SQL capability boundaries.
"""

from sqlbuster.core.engine import ClauseType, ClauseVariant, EvolvingEngine, SQLNode
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry, TableSchema
from sqlbuster.core.runner import BaseSQLRunner, MockRunner

__version__ = "0.1.0"
__all__ = [
    "EvolvingEngine",
    "ClauseType",
    "ClauseVariant",
    "SQLNode",
    "SchemaRegistry",
    "TableSchema",
    "ColumnSchema",
    "BaseSQLRunner",
    "MockRunner",
]
