"""
WHERE Clause Generator Module

This module implements the WHERE clause variant generator, supporting different forms of conditional expressions,
including equality comparison, range comparison, NULL check, SQL functions, etc.
支持代表性采样优化：如果某类型的列在WHERE子句上已成功，
only one representative variant is generated.
"""

import logging
import random
from collections import defaultdict
from typing import Any, Dict, List, Optional

from sqlbuster.clauses.base import BaseClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.function_registry import FunctionRegistry
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry

logger = logging.getLogger(__name__)


class WhereClauseGenerator(BaseClauseGenerator):
    """
    WHERE Clause Generator

    Generates various WHERE clause variants, including:
    - Equality condition (col = value)
    - Range condition (col > value, col < value)
    - NULL check (col IS NULL, col IS NOT NULL)
    - Fuzzy match (col LIKE pattern)
    - IN condition (col IN (value1, value2))
    - SQL function condition (UPPER(col) = value, ABS(col) > value)

    supports representative sampling optimization.
    """

    def __init__(self, registry: SchemaRegistry) -> None:
        """
        初始化WHERE Clause Generator

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
        Generate WHERE clause variants

        Generate corresponding SQL clause variant list based on clause type.
        如果type_success_cache中某类型已成功，only one representative variant is generated.

        Args:
            clause_type: Clause type (should be ClauseType.WHERE)
            registry: Schema registry (optional, priority given to initialized registry)
            type_success_cache: Type success cache for representative sampling

        Returns:
            WHERE clause variant list
        """
        if clause_type != ClauseType.WHERE:
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
            # Check cache
            cache_key = (table_name, ClauseType.WHERE, col_type)
            if type_success_cache and cache_key in type_success_cache:
                # Already succeeded, generate only one representative variant
                col = cols[0]
                variant_sql = self._generate_condition_with_functions(col)
                variants.append(
                    ClauseVariant(
                        clause_type=ClauseType.WHERE,
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
                    f"WHERE子句使用代表性采样: 类型={col_type.__name__}, 列={col.name}"
                )
            else:
                # Not succeeded, generate variants for all columns of this type
                for col in cols:
                    variant_sql = self._generate_condition_with_functions(col)
                    variants.append(
                        ClauseVariant(
                            clause_type=ClauseType.WHERE,
                            sql_fragment=variant_sql,
                            priority=1,
                            metadata={
                                "column": col.name,
                                "type": col_type.__name__,
                                "table": table_name,
                            },
                        )
                    )

        return variants

    def _generate_condition(self, col: ColumnSchema) -> str:
        """
        Generate WHERE condition for specified column (without function)

        Args:
            col: 列定义

        Returns:
            WHERE condition SQL fragment
        """
        col_name = col.name
        col_type = col.type

        # Generate a simple equality condition
        if col_type == str:
            return f"WHERE {col_name} = 'test'"
        elif col_type in (int, float):
            return f"WHERE {col_name} > 1"
        elif col_type == bool:
            return f"WHERE {col_name} = True"
        else:
            return f"WHERE {col_name} = 'test'"

    def _generate_condition_with_functions(self, col: ColumnSchema) -> str:
        """
        Generate WHERE condition for specified column (may use function)

        生成基础条件，有30%概率使用SQL函数包裹Column name。

        Args:
            col: 列定义

        Returns:
            WHERE condition SQL fragment（可能包含函数）
        """
        col_name = col.name
        col_type = col.type

        # 30%概率使用函数
        use_function = random.choice([True, False, False, False])

        if use_function:
            # Try to use appropriate function
            func_sql = self._apply_function_to_column(col)
            if func_sql and func_sql != col_name:
                # Generate condition based on column type
                if col_type == str:
                    return f"WHERE {func_sql} = 'test'"
                elif col_type in (int, float):
                    return f"WHERE {func_sql} > 1"
                elif col_type == bool:
                    return f"WHERE {func_sql} = True"

        # Do not use function, return basic condition
        return self._generate_condition(col)

    def _apply_function_to_column(self, col: ColumnSchema) -> str:
        """
        为列应用合适的SQL函数

        Args:
            col: 列定义

        Returns:
            应用函数后的SQL表达式，如果不适合则返回Column name本身
        """
        col_name = col.name
        col_type = col.type

        if col_type == str:
            # Use string functions for string columns
            string_funcs = self._function_registry.get_functions_by_category("string")
            # Select a single-parameter function
            for func in string_funcs:
                if len(func.parameters) == 1 and not func.is_aggregate:
                    param_name = func.parameters[0].name
                    result = self._function_registry.generate_sql(
                        func.name, {param_name: col_name}
                    )
                    if result:
                        return result

        elif col_type in (int, float):
            # Use numeric functions for numeric columns
            numeric_funcs = self._function_registry.get_functions_by_category("numeric")
            # Select a single-parameter function
            for func in numeric_funcs:
                if len(func.parameters) == 1 and not func.is_aggregate:
                    param_name = func.parameters[0].name
                    result = self._function_registry.generate_sql(
                        func.name, {param_name: col_name}
                    )
                    if result:
                        return result

        return col_name
