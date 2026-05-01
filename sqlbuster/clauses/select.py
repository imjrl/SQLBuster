"""
SELECT Clause Generator Module

This module implements the SELECT clause variant generator, supporting different forms of SELECT statements,
including SELECT *, specified columns, aggregate functions, SQL-92 standard functions, etc.
支持代表性采样优化（虽然SELECT不涉及列类型采样，但仍保持接口一致）。
"""

from typing import Any, Dict, List, Optional

from sqlbuster.clauses.base import BaseClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.function_registry import FunctionRegistry, SQLFunction
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry


class SelectClauseGenerator(BaseClauseGenerator):
    """
    SELECT Clause Generator

    Generates various SELECT clause variants, including:
    - SELECT * (query all columns)
    - SELECT Column name (查询单个列)
    - SELECT col1, col2 (query multiple columns)
    - SELECT aggregate function(col) (COUNT, SUM, AVG, MAX, MIN, etc.)
    - SELECT SQL function(col) (UPPER, LOWER, TRIM, etc.)

    支持代表性采样优化（虽然SELECT不涉及列类型采样）。
    Use FunctionRegistry to manage SQL functions.
    """

    def __init__(self, registry: SchemaRegistry) -> None:
        """
        初始化SELECT Clause Generator

        Args:
            registry: Schema registry
        """
        super().__init__(registry)
        self._function_registry = FunctionRegistry()

    def generate(
        self,
        clause_type: ClauseType,
        registry: Optional[SchemaRegistry] = None,
        type_success_cache: Optional[Dict] = None,
    ) -> List[ClauseVariant]:
        """
        Generate SELECT clause variants

        Args:
            clause_type: Clause type (should be ClauseType.SELECT)
            registry: Schema registry (optional, priority given to initialized registry)
            type_success_cache: 类型成功缓存（SELECT子句不使用，保持接口一致）

        Returns:
            SELECT clause variant list
        """
        if clause_type != ClauseType.SELECT:
            return []

        variants = []

        # Basic variant: SELECT *
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.SELECT,
                sql_fragment="SELECT *",
                priority=0,
                metadata={"type": "all_columns"},
            )
        )

        # Get table information以生成更具体的SELECT
        reg = registry or self._registry
        tables = reg.list_tables()

        if not tables:
            return variants

        table_name = tables[0]  # 使用第一个表
        columns = self.get_columns_from_table(table_name)

        if not columns:
            return variants

        # SELECT single column
        first_col = columns[0].name
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.SELECT,
                sql_fragment=f"SELECT {first_col}",
                priority=1,
                metadata={"type": "single_column", "column": first_col},
            )
        )

        # SELECT multiple columns
        if len(columns) >= 2:
            second_col = columns[1].name
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.SELECT,
                    sql_fragment=f"SELECT {first_col}, {second_col}",
                    priority=2,
                    metadata={
                        "type": "multiple_columns",
                        "columns": [first_col, second_col],
                    },
                )
            )

        # Use FunctionRegistry to generate aggregate function variants
        self._add_aggregate_function_variants(variants, columns)

        # Use FunctionRegistry to generate regular function variants (using functions in SELECT)
        self._add_sql_function_variants(variants, columns)

        # Sort by priority
        variants.sort(key=lambda x: x.priority)

        return variants

    def _add_aggregate_function_variants(
        self,
        variants: List[ClauseVariant],
        columns: List[ColumnSchema],
    ) -> None:
        """
        Add aggregate function variants

        Args:
            variants: Variant list (will be modified)
            columns: Column definition list
        """
        priority = 10  # Aggregate function priority starts from 10

        # COUNT(*) - Does not use specific column
        count_star_sql = self._function_registry.generate_sql("COUNT_STAR", {})
        if count_star_sql:
            variants.append(
                ClauseVariant(
                    clause_type=ClauseType.SELECT,
                    sql_fragment=f"SELECT {count_star_sql}",
                    priority=priority,
                    metadata={"type": "aggregate", "function": "COUNT", "column": "*"},
                )
            )
            priority += 1

        # Generate appropriate aggregate functions for each column type
        for col in columns:
            col_name = col.name
            col_type = col.type

            # COUNT(column) - Applies to all types
            count_sql = self._function_registry.generate_sql(
                "COUNT", {"expr": col_name}
            )
            if count_sql:
                variants.append(
                    ClauseVariant(
                        clause_type=ClauseType.SELECT,
                        sql_fragment=f"SELECT {count_sql}",
                        priority=priority,
                        metadata={
                            "type": "aggregate",
                            "function": "COUNT",
                            "column": col_name,
                        },
                    )
                )
                priority += 1

            # SUM and AVG - Only apply to numeric types
            if col_type in (int, float):
                sum_sql = self._function_registry.generate_sql(
                    "SUM", {"expr": col_name}
                )
                if sum_sql:
                    variants.append(
                        ClauseVariant(
                            clause_type=ClauseType.SELECT,
                            sql_fragment=f"SELECT {sum_sql}",
                            priority=priority,
                            metadata={
                                "type": "aggregate",
                                "function": "SUM",
                                "column": col_name,
                            },
                        )
                    )
                    priority += 1

                avg_sql = self._function_registry.generate_sql(
                    "AVG", {"expr": col_name}
                )
                if avg_sql:
                    variants.append(
                        ClauseVariant(
                            clause_type=ClauseType.SELECT,
                            sql_fragment=f"SELECT {avg_sql}",
                            priority=priority,
                            metadata={
                                "type": "aggregate",
                                "function": "AVG",
                                "column": col_name,
                            },
                        )
                    )
                    priority += 1

            # MAX and MIN - Apply to all types
            max_sql = self._function_registry.generate_sql("MAX", {"expr": col_name})
            if max_sql:
                variants.append(
                    ClauseVariant(
                        clause_type=ClauseType.SELECT,
                        sql_fragment=f"SELECT {max_sql}",
                        priority=priority,
                        metadata={
                            "type": "aggregate",
                            "function": "MAX",
                            "column": col_name,
                        },
                    )
                )
                priority += 1

            min_sql = self._function_registry.generate_sql("MIN", {"expr": col_name})
            if min_sql:
                variants.append(
                    ClauseVariant(
                        clause_type=ClauseType.SELECT,
                        sql_fragment=f"SELECT {min_sql}",
                        priority=priority,
                        metadata={
                            "type": "aggregate",
                            "function": "MIN",
                            "column": col_name,
                        },
                    )
                )
                priority += 1

    def _add_sql_function_variants(
        self,
        variants: List[ClauseVariant],
        columns: List[ColumnSchema],
    ) -> None:
        """
        Add SQL function variants (non-aggregate functions)

        Args:
            variants: Variant list (will be modified)
            columns: Column definition list
        """
        priority = 50  # Non-aggregate function priority starts from 50

        # Get string functions
        string_functions = self._function_registry.get_functions_by_category("string")

        for col in columns:
            col_name = col.name
            col_type = col.type

            # Only apply string functions to string columns
            if col_type == str:
                for func in string_functions:
                    # Skip functions that require multiple parameters (e.g. CONCAT)
                    if len(func.parameters) == 1 and not func.is_aggregate:
                        param_name = func.parameters[0].name
                        sql = self._function_registry.generate_sql(
                            func.name, {param_name: col_name}
                        )
                        if sql:
                            variants.append(
                                ClauseVariant(
                                    clause_type=ClauseType.SELECT,
                                    sql_fragment=f"SELECT {sql}",
                                    priority=priority,
                                    metadata={
                                        "type": "function",
                                        "function": func.name,
                                        "column": col_name,
                                        "category": func.category,
                                    },
                                )
                            )
                            priority += 1

        # Get numeric functions
        numeric_functions = self._function_registry.get_functions_by_category("numeric")

        for col in columns:
            col_name = col.name
            col_type = col.type

            # Only apply numeric functions to numeric columns
            if col_type in (int, float):
                # Single-parameter numeric functions (excluding CEILING/CEIL etc. that may have issues)
                single_param_funcs = [
                    f
                    for f in numeric_functions
                    if len(f.parameters) == 1 and not f.is_aggregate
                ]

                for func in single_param_funcs:
                    param_name = func.parameters[0].name
                    sql = self._function_registry.generate_sql(
                        func.name, {param_name: col_name}
                    )
                    if sql:
                        variants.append(
                            ClauseVariant(
                                clause_type=ClauseType.SELECT,
                                sql_fragment=f"SELECT {sql}",
                                priority=priority,
                                metadata={
                                    "type": "function",
                                    "function": func.name,
                                    "column": col_name,
                                    "category": func.category,
                                },
                            )
                        )
                        priority += 1
