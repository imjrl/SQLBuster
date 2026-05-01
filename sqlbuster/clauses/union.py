"""
UNION Clause Generator Module

This module implements the UNION clause variant generator, supporting UNION/UNION ALL variants.
UNION syntax: SELECT ... FROM table1 UNION SELECT ... FROM table2

supports representative sampling optimization.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlbuster.clauses.base import BaseClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry

logger = logging.getLogger(__name__)


class UnionClauseGenerator(BaseClauseGenerator):
    """
    UNION Clause Generator

    Generates various UNION clause variants, including:
    - UNION: Deduplicated union
    - UNION ALL: Non-deduplicated union

    supports representative sampling optimization.

    UNION is used to merge result sets from two or more SELECT statements.
    Note: UNION automatically deduplicates, UNION ALL retains all records (including duplicates).
    """

    def generate(
        self,
        clause_type: ClauseType,
        registry: Optional[SchemaRegistry] = None,
        type_success_cache: Optional[Dict] = None,
    ) -> List[ClauseVariant]:
        """
        Generate UNION clause variants

        Args:
            clause_type: Clause type (should be ClauseType.UNION)
            registry: Schema registry (optional, priority given to initialized registry)
            type_success_cache: Type success cache for representative sampling

        Returns:
            UNION clause variant list
        """
        if clause_type != ClauseType.UNION:
            return []

        variants: List[ClauseVariant] = []

        # Get all tables
        reg = registry or self._registry
        tables = reg.list_tables()

        if len(tables) < 2:
            # At least two tables are required for UNION
            logger.info("UNION生成器：需要至少2个表，当前只有%d个表", len(tables))
            return variants

        main_table = tables[0]
        columns = self.get_columns_from_table(main_table)

        if not columns:
            return variants

        # Check cache
        cache_key = (main_table, ClauseType.UNION, str)
        if type_success_cache and cache_key in type_success_cache:
            # Already succeeded, generate only one representative variant
            other_table = tables[1]
            union_sql = f"UNION SELECT {columns[0].name} FROM {other_table}"
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.UNION,
                    sql_fragment=f"UNION SELECT {columns[0].name} FROM {other_table}",
                    priority=1,
                    metadata={
                        "type": "union_representative",
                        "main_table": main_table,
                        "union_table": other_table,
                        "representative": True,
                    },
                )
            )
            logger.info(f"UNION子句使用代表性采样: 主表={main_table}")
            return variants

        # Generate multiple UNION variants

        # Generate UNION variants for each other table
        for other_table_name in tables[1:]:
            other_columns = self.get_columns_from_table(other_table_name)
            if not other_columns:
                continue

            # UNION - Use same columns
            union_sql = f"UNION SELECT {columns[0].name} FROM {other_table_name}"
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.UNION,
                    sql_fragment=union_sql,
                    priority=0,
                    metadata={
                        "type": "union",
                        "main_table": main_table,
                        "union_table": other_table_name,
                        "column": columns[0].name,
                    },
                )
            )

            # UNION ALL
            union_all_sql = (
                f"UNION ALL SELECT {columns[0].name} FROM {other_table_name}"
            )
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.UNION,
                    sql_fragment=union_all_sql,
                    priority=1,
                    metadata={
                        "type": "union_all",
                        "main_table": main_table,
                        "union_table": other_table_name,
                        "column": columns[0].name,
                    },
                )
            )

            # UNION SELECT * (if column counts match)
            if len(columns) == len(other_columns):
                union_all_sql = f"UNION SELECT * FROM {other_table_name}"
                variants.append(
                    ClauseVariant(
                        clause_type=ClauseType.UNION,
                        sql_fragment=union_all_sql,
                        priority=2,
                        metadata={
                            "type": "union_select_all",
                            "main_table": main_table,
                            "union_table": other_table_name,
                        },
                    )
                )

        return variants
