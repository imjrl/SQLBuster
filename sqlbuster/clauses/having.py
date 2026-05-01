"""
HAVING Clause Generator Module

This module implements the HAVING clause variant generator, supporting HAVING clauses with aggregate conditions.
Note: HAVING clause must be used after GROUP BY clause.
支持代表性采样优化：如果某类型的列在HAVING子句上已成功，
only one representative variant is generated.
"""

import logging
from collections import defaultdict
from typing import Any, Dict, List, Optional

from sqlbuster.clauses.base import BaseClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry

logger = logging.getLogger(__name__)


class HavingClauseGenerator(BaseClauseGenerator):
    """
    HAVING Clause Generator

    Generates various HAVING clause variants, including:
    - COUNT aggregate condition (HAVING COUNT(*) > 1)
    - SUM aggregate condition (HAVING SUM(col) > 100)
    - AVG aggregate condition (HAVING AVG(col) > 50)

    Note:
        HAVING clause is usually used after GROUP BY, context must be considered when generating variants.
        本生成器生成的变体可以在有GROUP BY的SQL中使用。

    supports representative sampling optimization.
    """

    def generate(
        self,
        clause_type: ClauseType,
        registry: Optional[SchemaRegistry] = None,
        type_success_cache: Optional[Dict] = None,
    ) -> List[ClauseVariant]:
        """
        Generate HAVING clause variants

        Generate corresponding SQL clause variant list based on clause type.
        如果type_success_cache中某类型已成功，only one representative variant is generated.

        Args:
            clause_type: Clause type (should be ClauseType.HAVING)
            registry: Schema registry (optional, priority given to initialized registry)
            type_success_cache: Type success cache for representative sampling

        Returns:
            HAVING clause variant list
        """
        if clause_type != ClauseType.HAVING:
            return []

        variants: List[ClauseVariant] = []

        # Use common logic to get tables and columns grouped by type
        table_name, columns, columns_by_type = self._get_table_and_group_by_type(
            registry
        )

        if table_name is None:
            return variants

        # HAVING COUNT(*) - Not column-specific, always generate
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.HAVING,
                sql_fragment="HAVING COUNT(*) > 1",
                priority=0,
                metadata={"type": "aggregate", "table": table_name},
            )
        )

        # Group columns by type (for conditions that need columns)
        # columns_by_type already returned by _get_table_and_group_by_type

        # For each type, check if there is already a success record
        for col_type, cols in columns_by_type.items():
            cache_key = (table_name, ClauseType.HAVING, col_type)
            if type_success_cache and cache_key in type_success_cache:
                # Already succeeded, generate only one representative variant
                col = cols[0]
                variant_sql = self._generate_aggregate_condition(col, "COUNT")
                variants.append(
                    ClauseVariant(
                        clause_type=ClauseType.HAVING,
                        sql_fragment=variant_sql,
                        priority=1,
                        metadata={
                            "column": col.name,
                            "type": col_type.__name__,
                            "representative": True,
                            "table": table_name,
                        },
                    )
                )
                logger.info(
                    f"HAVING子句使用代表性采样: 类型={col_type.__name__}, 列={col.name}"
                )
            else:
                # Not succeeded, generate variants for all columns of this type
                for col in cols:
                    for agg_func in ["COUNT", "SUM", "AVG", "MAX", "MIN"]:
                        # Only numeric types are suitable for SUM, AVG, MAX, MIN
                        if agg_func in [
                            "SUM",
                            "AVG",
                            "MAX",
                            "MIN",
                        ] and col.type not in (int, float):
                            continue
                        variant_sql = self._generate_aggregate_condition(col, agg_func)
                        variants.append(
                            ClauseVariant(
                                clause_type=ClauseType.HAVING,
                                sql_fragment=variant_sql,
                                priority=1,
                                metadata={
                                    "column": col.name,
                                    "type": col.type.__name__,
                                    "table": table_name,
                                },
                            )
                        )

        return variants

    def _generate_aggregate_condition(self, col: ColumnSchema, agg_func: str) -> str:
        """
        Generate aggregate condition for specified column

        Args:
            col: 列定义
            agg_func: Aggregate function name (COUNT, SUM, AVG, MAX, MIN)

        Returns:
            HAVING条件SQL片段
        """
        if agg_func == "COUNT":
            return f"HAVING COUNT({col.name}) > 1"
        elif agg_func == "SUM":
            return f"HAVING SUM({col.name}) > 100"
        elif agg_func == "AVG":
            return f"HAVING AVG({col.name}) > 50"
        elif agg_func == "MAX":
            return f"HAVING MAX({col.name}) > 1000"
        elif agg_func == "MIN":
            return f"HAVING MIN({col.name}) < 10"
        return f"HAVING COUNT({col.name}) > 1"
