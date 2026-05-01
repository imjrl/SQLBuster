"""
Clause Generator Base Class Module

This module defines the abstract base class for clause generators. All concrete clause generators must inherit from this class.
Provides unified interfaces and common utility methods.
"""

from abc import ABC, abstractmethod
from collections import defaultdict
from typing import Any, Callable, Dict, List, Optional, Tuple

from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry


class BaseClauseGenerator(ABC):
    """
    Clause Generator Abstract Base Class

    Base class for all clause generators, defines unified interfaces.
    Each concrete generator is responsible for generating SQL clause variants of a specific type.
    """

    def __init__(self, registry: SchemaRegistry) -> None:
        """
        Initialize clause generator

        Args:
            registry: Schema registry，用于Get table schema和列信息
        """
        self._registry = registry

    @abstractmethod
    def generate(
        self,
        clause_type: ClauseType,
        registry: Optional[SchemaRegistry] = None,
        type_success_cache: Optional[Dict] = None,
    ) -> List[ClauseVariant]:
        """
        生成子句变体

        Generate corresponding SQL clause variant list based on clause type.
        支持代表性采样优化：如果type_success_cache中某类型已成功，
        only one representative variant is generated.

        Args:
            clause_type: Clause type
            registry: Schema registry (optional, priority given to initialized registry)
            type_success_cache: Type success cache for representative sampling优化

        Returns:
            子句变体列表
        """
        pass

    def _get_table_and_group_by_type(
        self, registry: Optional[SchemaRegistry] = None
    ) -> Tuple[Optional[str], List[ColumnSchema], Dict[type, List[ColumnSchema]]]:
        """
        Get first table and group columns by type (common logic)

        Most generators need: get table, get columns, group by type.
        This method encapsulates this common flow.

        Args:
            registry: Schema registry（可选，默认使用self._registry）

        Returns:
            (table_name, columns, columns_by_type) 元组
            If failed, return (None, [], {})
        """
        reg = registry or self._registry
        tables = reg.list_tables()

        if not tables:
            return None, [], {}

        table_name = tables[0]
        table = reg.get_table(table_name)
        if not table:
            return None, [], {}

        columns = table.columns
        if not columns:
            return table_name, [], {}

        # 按类型分组列
        columns_by_type: Dict[type, List[ColumnSchema]] = defaultdict(list)
        for col in columns:
            columns_by_type[col.type].append(col)

        return table_name, columns, columns_by_type

    def get_random_column_from_table(self, table_name: str) -> Optional[ColumnSchema]:
        """
        Get a random column from the specified table

        Args:
            table_name: Table name

        Returns:
            列定义，如果表不存在或没有列则返回None
        """
        return self._registry.get_random_column(table_name)

    def get_columns_from_table(self, table_name: str) -> List[ColumnSchema]:
        """
        Get all columns from specified table

        Args:
            table_name: Table name

        Returns:
            Column definition list，Return empty list if table does not exist
        """
        table = self._registry.get_table(table_name)
        if table is None:
            return []
        return table.columns

    def get_first_column(self, table_name: str) -> Optional[ColumnSchema]:
        """
        Get first column of specified table

        Args:
            table_name: Table name

        Returns:
            第一列的定义，Return None if no columns
        """
        columns = self.get_columns_from_table(table_name)
        if columns:
            return columns[0]
        return None


def register_clause_generator(
    engine: Any,
    clause_type: ClauseType,
    generator_class: type,
    registry: SchemaRegistry,
) -> None:
    """
    Register clause generator to engine

    Factory function to create generator instance and register to evolution engine.

    Args:
        engine: 进化Engine实例
        clause_type: Clause type
        generator_class: Generator class (inherits from BaseClauseGenerator)
        registry: Schema registry
    """
    generator = generator_class(registry)

    # Create wrapper function to support new interface
    def generator_wrapper(
        ct: ClauseType, reg: SchemaRegistry, cache: Optional[Dict] = None
    ) -> List[ClauseVariant]:
        # Call generator's generate method, pass cache parameters
        return generator.generate(ct, registry=reg, type_success_cache=cache)  # type: ignore[no-any-return]

    engine.register_clause_generator(clause_type, generator_wrapper)
