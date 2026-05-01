"""
WINDOW Clause Generator Module

This module implements the WINDOW clause variant generator, supporting window function related variants.
WINDOW syntax: WINDOW window_name AS (PARTITION BY col ORDER BY col)

supports representative sampling optimization.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlbuster.clauses.base import BaseClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry

logger = logging.getLogger(__name__)


class WindowClauseGenerator(BaseClauseGenerator):
    """
    WINDOW Clause Generator

    Generates various WINDOW clause variants, including:
    - Simple window: WINDOW w AS (PARTITION BY col)
    - Window with sorting: WINDOW w AS (PARTITION BY col ORDER BY col)
    - Multiple window definitions

    supports representative sampling optimization.

    WINDOW clause is used to define named windows, which can be referenced by window functions in SELECT.
    Example: SELECT ROW_NUMBER() OVER w FROM table WINDOW w AS (PARTITION BY col)
    """

    def generate(
        self,
        clause_type: ClauseType,
        registry: Optional[SchemaRegistry] = None,
        type_success_cache: Optional[Dict] = None,
    ) -> List[ClauseVariant]:
        """
        Generate WINDOW clause variants

        Args:
            clause_type: Clause type (should be ClauseType.WINDOW)
            registry: Schema registry (optional, priority given to initialized registry)
            type_success_cache: Type success cache for representative sampling

        Returns:
            WINDOW clause variant list
        """
        if clause_type != ClauseType.WINDOW:
            return []

        variants: List[ClauseVariant] = []

        # Use common logic to get tables and columns grouped by type
        table_name, columns, columns_by_type = self._get_table_and_group_by_type(
            registry
        )

        if table_name is None or not columns:
            return variants

        # Check cache
        cache_key = (table_name, ClauseType.WINDOW, str)
        if type_success_cache and cache_key in type_success_cache:
            # Already succeeded, generate only one representative variant
            col = columns[0]
            window_sql = f"WINDOW w AS (PARTITION BY {col.name})"
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.WINDOW,
                    sql_fragment=window_sql,
                    priority=1,
                    metadata={
                        "type": "window_representative",
                        "table": table_name,
                        "representative": True,
                    },
                )
            )
            logger.info(f"WINDOW子句使用代表性采样: 表={table_name}")
            return variants

        # 生成多个WINDOW变体

        # 1. Simple PARTITION BY window
        if columns:
            first_col = columns[0]
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.WINDOW,
                    sql_fragment=f"WINDOW w AS (PARTITION BY {first_col.name})",
                    priority=0,
                    metadata={
                        "type": "window_partition",
                        "table": table_name,
                        "partition_column": first_col.name,
                    },
                )
            )

        # 2. PARTITION BY + ORDER BY window
        if len(columns) >= 2:
            first_col = columns[0]
            second_col = columns[1]
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.WINDOW,
                    sql_fragment=f"WINDOW w AS (PARTITION BY {first_col.name} ORDER BY {second_col.name})",
                    priority=1,
                    metadata={
                        "type": "window_partition_order",
                        "table": table_name,
                        "partition_column": first_col.name,
                        "order_column": second_col.name,
                    },
                )
            )

        # 3. Window with only ORDER BY
        if columns:
            first_col = columns[0]
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.WINDOW,
                    sql_fragment=f"WINDOW w AS (ORDER BY {first_col.name})",
                    priority=2,
                    metadata={
                        "type": "window_order",
                        "table": table_name,
                        "order_column": first_col.name,
                    },
                )
            )

        return variants
