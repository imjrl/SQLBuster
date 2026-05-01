"""
JOIN Clause Generator Module

This module implements the JOIN clause variant generator, supporting different types of JOIN variants.
JOIN syntax: FROM table1 [INNER/LEFT/RIGHT] JOIN table2 ON table1.col = table2.col

supports representative sampling optimization.
"""

import logging
from typing import Any, Dict, List, Optional

from sqlbuster.clauses.base import BaseClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry

logger = logging.getLogger(__name__)


class JoinClauseGenerator(BaseClauseGenerator):
    """
    JOIN Clause Generator

    Generates various JOIN clause variants, including:
    - INNER JOIN: Inner join
    - LEFT JOIN: Left join
    - RIGHT JOIN: Right join (if supported)
    - CROSS JOIN: Cross join

    supports representative sampling optimization.
    """

    def generate(
        self,
        clause_type: ClauseType,
        registry: Optional[SchemaRegistry] = None,
        type_success_cache: Optional[Dict] = None,
    ) -> List[ClauseVariant]:
        """
        Generate JOIN clause variants

        Args:
            clause_type: Clause type (should be ClauseType.JOIN)
            registry: Schema registry (optional, priority given to initialized registry)
            type_success_cache: Type success cache for representative sampling

        Returns:
            JOIN clause variant list
        """
        if clause_type != ClauseType.JOIN:
            return []

        variants: List[ClauseVariant] = []

        # Get all tables
        reg = registry or self._registry
        tables = reg.list_tables()

        if len(tables) < 2:
            # At least two tables are required for JOIN
            logger.info("JOIN生成器：需要至少2个表，当前只有%d个表", len(tables))
            return variants

        # Use first table as main table (already in FROM), other tables as JOIN targets
        main_table = tables[0]
        main_table_schema = reg.get_table(main_table)

        if not main_table_schema or not main_table_schema.columns:
            return variants

        # Check cache（基于主Table name）
        cache_key = (main_table, ClauseType.JOIN, str)
        if type_success_cache and cache_key in type_success_cache:
            # Already succeeded, generate only one representative variant
            other_table = tables[1]
            join_col = main_table_schema.columns[0]
            join_sql = f"JOIN {other_table} ON {main_table}.{join_col.name} = {other_table}.{join_col.name}"
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.JOIN,
                    sql_fragment=join_sql,
                    priority=1,
                    metadata={
                        "type": "join_representative",
                        "main_table": main_table,
                        "join_table": other_table,
                        "representative": True,
                    },
                )
            )
            logger.info(f"JOIN子句使用代表性采样: 主表={main_table}")
            return variants

        # 生成多种JOIN变体

        # Generate different types of JOIN for each other table
        for other_table_name in tables[1:]:
            other_table_schema = reg.get_table(other_table_name)
            if not other_table_schema:
                continue

            # Find columns that can be JOINed (same name columns)
            join_columns = self._find_join_columns(
                main_table_schema.columns, other_table_schema.columns
            )

            if not join_columns:
                # If no same-name columns, use CROSS JOIN or first column
                join_columns = [
                    (main_table_schema.columns[0], other_table_schema.columns[0])
                ]

            # Generate JOIN variants for each pair of joinable columns
            for main_col, other_col in join_columns[:2]:  # 限制变体数量
                # INNER JOIN
                inner_join_sql = f"JOIN {other_table_name} ON {main_table}.{main_col.name} = {other_table_name}.{other_col.name}"
                variants.append(
                    ClauseVariant(
                        clause_type=ClauseType.JOIN,
                        sql_fragment=inner_join_sql,
                        priority=0,
                        metadata={
                            "type": "inner_join",
                            "main_table": main_table,
                            "join_table": other_table_name,
                            "main_column": main_col.name,
                            "join_column": other_col.name,
                        },
                    )
                )

                # LEFT JOIN
                left_join_sql = f"LEFT JOIN {other_table_name} ON {main_table}.{main_col.name} = {other_table_name}.{other_col.name}"
                variants.append(
                    ClauseVariant(
                        clause_type=ClauseType.JOIN,
                        sql_fragment=left_join_sql,
                        priority=1,
                        metadata={
                            "type": "left_join",
                            "main_table": main_table,
                            "join_table": other_table_name,
                            "main_column": main_col.name,
                            "join_column": other_col.name,
                        },
                    )
                )

        return variants

    def _find_join_columns(
        self, main_columns: List[ColumnSchema], other_columns: List[ColumnSchema]
    ) -> List[tuple]:
        """
        Find column pairs that can be used for JOIN (same name columns)

        Args:
            main_columns: Main table column list
            other_columns: Other table column list

        Returns:
            List of joinable column pairs [(main_col, other_col), ...]
        """
        join_pairs = []

        # 按Column name匹配
        other_col_dict = {col.name: col for col in other_columns}

        for main_col in main_columns:
            if main_col.name in other_col_dict:
                join_pairs.append((main_col, other_col_dict[main_col.name]))

        return join_pairs
