"""
ORDER BY Clause Generator Module

This module implements the ORDER BY clause variant generator, supporting ascending and descending sort clauses.
Supports representative sampling optimization: if a column of a type has succeeded on ORDER BY clause,
only one representative variant is generated.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional

from sqlbuster.clauses.base import BaseClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry

logger = logging.getLogger(__name__)


class OrderByClauseGenerator(BaseClauseGenerator):
    """
    ORDER BY Clause Generator

    Generates various ORDER BY clause variants, including:
    - Single column ascending (ORDER BY col)
    - Single column descending (ORDER BY col DESC)
    - Multi-column sorting (ORDER BY col1, col2)
    - Mixed sorting (ORDER BY col1 ASC, col2 DESC)

    supports representative sampling optimization.
    """

    def generate(
        self,
        clause_type: ClauseType,
        registry: Optional[SchemaRegistry] = None,
        type_success_cache: Optional[Dict] = None,
    ) -> List[ClauseVariant]:
        """
        Generate ORDER BY clause variants

        Generate corresponding SQL clause variant list based on clause type.
        如果type_success_cache中某类型已成功，only one representative variant is generated.

        Args:
            clause_type: Clause type (should be ClauseType.ORDER_BY)
            registry: Schema registry (optional, priority given to initialized registry)
            type_success_cache: Type success cache for representative sampling

        Returns:
            ORDER BY clause variant list
        """
        if clause_type != ClauseType.ORDER_BY:
            return []

        variants: List[ClauseVariant] = []

        # Use common logic to get tables and columns grouped by type
        table_name, columns, columns_by_type = self._get_table_and_group_by_type(
            registry
        )

        if table_name is None:
            return variants

        # For each type, check if there is already a success record
        for col_type, cols in columns_by_type.items():
            cache_key = (table_name, ClauseType.ORDER_BY, col_type)
            if type_success_cache and cache_key in type_success_cache:
                # Already succeeded, generate only one representative variant (ascending)
                col = cols[0]
                variants.append(
                    ClauseVariant(
                        clause_type=ClauseType.ORDER_BY,
                        sql_fragment=f"ORDER BY {col.name}",
                        priority=0,
                        metadata={
                            "column": col.name,
                            "type": col_type.__name__,
                            "representative": True,
                            "table": table_name,
                        },
                    )
                )
                logger.info(
                    f"ORDER BY子句使用代表性采样: 类型={col_type.__name__}, 列={col.name}"
                )
            else:
                # Not succeeded, generate variants for all columns of this type
                for col in cols:
                    # Single column ascending
                    variants.append(
                        ClauseVariant(
                            clause_type=ClauseType.ORDER_BY,
                            sql_fragment=f"ORDER BY {col.name}",
                            priority=0,
                            metadata={
                                "column": col.name,
                                "type": col_type.__name__,
                                "table": table_name,
                            },
                        )
                    )

                    # Single column descending
                    variants.append(
                        ClauseVariant(
                            clause_type=ClauseType.ORDER_BY,
                            sql_fragment=f"ORDER BY {col.name} DESC",
                            priority=1,
                            metadata={
                                "column": col.name,
                                "type": col_type.__name__,
                                "table": table_name,
                            },
                        )
                    )

        # Multi-column sorting (does not use representative sampling, as it spans types)
        # Only generate when not using representative sampling
        if len(columns) >= 2:
            first_col = columns[0]
            second_col = columns[1]
            first_type = first_col.type.__name__
            second_type = second_col.type.__name__

            # Check if multi-column sorting for this type is already cached
            cache_key1 = (table_name, ClauseType.ORDER_BY, first_col.type)
            cache_key2 = (table_name, ClauseType.ORDER_BY, second_col.type)

            if not (
                type_success_cache
                and cache_key1 in type_success_cache
                and cache_key2 in type_success_cache
            ):
                variants.append(
                    ClauseVariant(
                        clause_type=ClauseType.ORDER_BY,
                        sql_fragment=f"ORDER BY {first_col.name}, {second_col.name}",
                        priority=2,
                        metadata={
                            "columns": [first_col.name, second_col.name],
                            "types": [first_type, second_type],
                            "table": table_name,
                        },
                    )
                )

                # Mixed sorting
                variants.append(
                    ClauseVariant(
                        clause_type=ClauseType.ORDER_BY,
                        sql_fragment=f"ORDER BY {first_col.name} ASC, {second_col.name} DESC",
                        priority=3,
                        metadata={
                            "columns": [first_col.name, second_col.name],
                            "types": [first_type, second_type],
                            "table": table_name,
                        },
                    )
                )

        return variants
