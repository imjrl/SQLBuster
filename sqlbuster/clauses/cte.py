"""
CTE (Common Table Expression) Clause Generator Module

This module implements the WITH clause variant generator, supporting CTE (Common Table Expression) variants.
CTE syntax: WITH cte_name AS (SELECT ... FROM ...) SELECT * FROM cte_name

supports representative sampling optimization.
"""

from typing import Any, Dict, List, Optional

from sqlbuster.clauses.base import BaseClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry


class CteClauseGenerator(BaseClauseGenerator):
    """
    CTE (Common Table Expression) Clause Generator

    Generates various WITH clause variants, including:
    - Simple CTE: WITH cte AS (SELECT * FROM table)
    - Column selection CTE: WITH cte AS (SELECT col1, col2 FROM table)
    - Conditional CTE: WITH cte AS (SELECT * FROM table WHERE condition)

    supports representative sampling optimization.
    """

    def generate(
        self,
        clause_type: ClauseType,
        registry: Optional[SchemaRegistry] = None,
        type_success_cache: Optional[Dict] = None,
    ) -> List[ClauseVariant]:
        """
        Generate CTE (WITH clause) variants

        Args:
            clause_type: Clause type (should be ClauseType.CTE)
            registry: Schema registry (optional, priority given to initialized registry)
            type_success_cache: Type success cache for representative sampling

        Returns:
            CTE clause variant list
        """
        if clause_type != ClauseType.CTE:
            return []

        variants: List[ClauseVariant] = []

        # Get table information
        reg = registry or self._registry
        tables = reg.list_tables()

        if not tables:
            return variants

        table_name = tables[0]
        columns = self.get_columns_from_table(table_name)

        if not columns:
            return variants

        # Check cache（CTE主要依赖于表，而不是列类型）
        cache_key = (table_name, ClauseType.CTE, str)
        if type_success_cache and cache_key in type_success_cache:
            # Already succeeded, generate only one representative variant
            col = columns[0]
            cte_sql = (
                f"WITH cte AS (SELECT {col.name} FROM {table_name}) SELECT * FROM cte"
            )
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.CTE,
                    sql_fragment=cte_sql,
                    priority=1,
                    metadata={
                        "type": "cte_representative",
                        "table": table_name,
                        "representative": True,
                    },
                )
            )
            return variants

        # Generate multiple CTE variants

        # 1. Simple CTE: WITH cte AS (SELECT * FROM table)
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.CTE,
                sql_fragment=f"WITH cte AS (SELECT * FROM {table_name}) SELECT * FROM cte",
                priority=0,
                metadata={"type": "cte_simple", "table": table_name},
            )
        )

        # 2. Column selection CTE: WITH cte AS (SELECT col1, col2 FROM table)
        if len(columns) >= 2:
            col1 = columns[0].name
            col2 = columns[1].name
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.CTE,
                    sql_fragment=f"WITH cte AS (SELECT {col1}, {col2} FROM {table_name}) SELECT * FROM cte",
                    priority=1,
                    metadata={
                        "type": "cte_columns",
                        "table": table_name,
                        "columns": [col1, col2],
                    },
                )
            )

        # 3. Single column CTE
        first_col = columns[0]
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.CTE,
                sql_fragment=f"WITH cte AS (SELECT {first_col.name} FROM {table_name}) SELECT {first_col.name} FROM cte",
                priority=2,
                metadata={
                    "type": "cte_single_column",
                    "table": table_name,
                    "column": first_col.name,
                },
            )
        )

        return variants
