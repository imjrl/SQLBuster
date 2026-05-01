"""
Nested Subquery Clause Generator Module

This module implements the nested subquery variant generator, supporting generation of
SQL statements with nested subqueries, including:
- Derived table nested subqueries (FROM clause)
- Subqueries in WHERE clause
- Scalar subqueries in SELECT clause

Supports representative sampling optimization.
"""

from typing import Any, Dict, List, Optional, override

from sqlbuster.clauses.base import BaseClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry


class NestedSubqueryClauseGenerator(BaseClauseGenerator):
    """
    Nested Subquery Clause Generator

    Generates variants of SQL statements with nested subqueries.
    Supports representative sampling optimization.
    """

    @override
    def generate(
        self,
        clause_type: ClauseType,
        registry: Optional[SchemaRegistry] = None,
        type_success_cache: Optional[Dict[Any, Any]] = None,
    ) -> List[ClauseVariant]:
        """
        Generate nested subquery clause variants

        Args:
            clause_type: Clause type (should be ClauseType.NESTED_SUBQUERY)
            registry: Schema registry (optional, uses initialized registry by default)
            type_success_cache: Type success cache for representative sampling

        Returns:
            List of nested subquery clause variants
        """
        if clause_type != ClauseType.NESTED_SUBQUERY:
            return []

        variants: List[ClauseVariant] = []
        reg = registry or self._registry
        tables = reg.list_tables()

        if len(tables) < 1:
            return variants

        table_name = tables[0]
        columns = self.get_columns_from_table(table_name)

        if not columns:
            return variants

        cache_key = (table_name, ClauseType.NESTED_SUBQUERY, str)
        if type_success_cache and cache_key in type_success_cache:
            representative_variant = self._generate_representative_variant(
                table_name, columns
            )
            if representative_variant:
                variants.append(representative_variant)
            return variants

        self._add_derived_table_nested_variants(variants, table_name, columns)
        self._add_where_nested_subquery_variants(variants, table_name, columns, reg)
        self._add_scalar_nested_subquery_variants(variants, table_name, columns)

        return variants

    def _generate_representative_variant(
        self, table_name: str, columns: List[ColumnSchema]
    ) -> Optional[ClauseVariant]:
        """Generate a single representative nested subquery variant"""
        if not columns:
            return None

        col = columns[0]
        nested_sql = (
            f"SELECT * FROM "
            f"(SELECT * FROM "
            f"(SELECT {col.name} FROM {table_name}) AS sub1"
            f") AS sub2"
        )
        return ClauseVariant(
            clause_type=ClauseType.NESTED_SUBQUERY,
            sql_fragment=nested_sql,
            priority=1,
            metadata={
                "type": "nested_subquery_representative",
                "table": table_name,
                "representative": True,
            },
        )

    def _add_derived_table_nested_variants(
        self,
        variants: List[ClauseVariant],
        table_name: str,
        columns: List[ColumnSchema],
    ) -> None:
        """Add derived table (FROM clause) nested subquery variants"""
        if not columns:
            return

        col = columns[0]
        variant1_sql = f"SELECT * FROM " f"(SELECT * FROM {table_name}) AS sub"
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.NESTED_SUBQUERY,
                sql_fragment=variant1_sql,
                priority=0,
                metadata={"type": "derived_table_single_nested", "table": table_name},
            )
        )

        variant2_sql = (
            f"SELECT * FROM "
            f"(SELECT * FROM "
            f"(SELECT {col.name} FROM {table_name}) AS sub1"
            f") AS sub2"
        )
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.NESTED_SUBQUERY,
                sql_fragment=variant2_sql,
                priority=1,
                metadata={"type": "derived_table_double_nested", "table": table_name},
            )
        )

        variant3_sql = (
            f"SELECT * FROM "
            f"(SELECT * FROM "
            f"(SELECT * FROM "
            f"(SELECT {col.name} FROM {table_name}) AS sub1"
            f") AS sub2"
            f") AS sub3"
        )
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.NESTED_SUBQUERY,
                sql_fragment=variant3_sql,
                priority=2,
                metadata={"type": "derived_table_triple_nested", "table": table_name},
            )
        )

    def _add_where_nested_subquery_variants(
        self,
        variants: List[ClauseVariant],
        table_name: str,
        columns: List[ColumnSchema],
        registry: SchemaRegistry,
    ) -> None:
        """Add WHERE clause nested subquery variants"""
        if not columns:
            return

        col = columns[0]
        first_col_name = col.name

        where_nested_sql = (
            f"SELECT * FROM {table_name} "
            f"WHERE {first_col_name} IN "
            f"(SELECT {first_col_name} FROM "
            f"(SELECT {first_col_name} FROM {table_name}) AS sub"
            f")"
        )
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.NESTED_SUBQUERY,
                sql_fragment=where_nested_sql,
                priority=3,
                metadata={"type": "where_nested_subquery", "table": table_name},
            )
        )

    def _add_scalar_nested_subquery_variants(
        self,
        variants: List[ClauseVariant],
        table_name: str,
        columns: List[ColumnSchema],
    ) -> None:
        """Add scalar nested subquery variants (SELECT clause)"""
        if len(columns) < 2:
            return

        col1 = columns[0]
        col2 = columns[1]
        scalar_nested_sql = (
            f"SELECT "
            f"(SELECT {col1.name} FROM "
            f"(SELECT {col1.name} FROM {table_name} WHERE {col2.name} = 'test') AS sub"
            f") AS sub_col, "
            f"{col2.name} "
            f"FROM {table_name}"
        )
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.NESTED_SUBQUERY,
                sql_fragment=scalar_nested_sql,
                priority=4,
                metadata={"type": "scalar_nested_subquery", "table": table_name},
            )
        )
