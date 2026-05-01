"""
Schema Registry Module

This module defines the SQL schema registry (SchemaRegistry) and related data structures,
used to manage database table structure information and provide schema support for SQL generation.
"""

import random
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Type

from sqlbuster.utils.errors import ColumnNotFoundError, SchemaError, TableNotFoundError


@dataclass
class ColumnSchema:
    """
    Column Schema Definition

    Represents the structure information of a column in a database table, including column name, data type, and nullability.
    """

    name: str
    """Column name"""

    type: Type
    """Python type of the column (e.g. int, str, float)"""

    is_nullable: bool = True
    """Whether the column allows NULL values, defaults to True"""


@dataclass
class TableSchema:
    """
    Table Schema Definition

    Represents the structure information of a database table, including table name and list of column definitions.
    """

    name: str
    """Table name"""

    columns: List[ColumnSchema] = field(default_factory=list)
    """List of column definitions for the table"""


class SchemaRegistry:
    """
    SQL Schema Registry

    Manages database table structure information, provides table registration, querying, and mock data generation.
    Supports registering multiple tables, each containing definitions for multiple columns.
    """

    def __init__(self) -> None:
        """Initialize schema registry"""
        self._tables: Dict[str, TableSchema] = {}

    def register_table(self, name: str, columns: List[ColumnSchema]) -> None:
        """
        Register table schema.

        Registers table structure information to the registry. If the table already exists, it will be overwritten.

        Args:
            name: Table name
            columns: List of column definitions

        Raises:
            SchemaError: If table name or column definitions are invalid
        """
        if not name or not isinstance(name, str):
            raise SchemaError("Table name must be a valid non-empty string")

        if not isinstance(columns, list):
            raise SchemaError("Column definitions must be a list")

        table = TableSchema(name=name, columns=columns)
        self._tables[name] = table

    def get_table(self, name: str) -> Optional[TableSchema]:
        """
        Get table schema.

        Args:
            name: Table name

        Returns:
            Table schema, or None if table does not exist
        """
        return self._tables.get(name)

    def list_tables(self) -> List[str]:
        """
        List all registered table names.

        Returns:
            List of table names, ordered by registration sequence
        """
        return list(self._tables.keys())

    def get_random_column(self, table: str) -> Optional[ColumnSchema]:
        """
        Get a random column from the specified table.

        Args:
            table: Table name

        Returns:
            Randomly selected column definition, or None if table does not exist or has no columns

        Raises:
            TableNotFoundError: If table does not exist (optional, design-dependent)
        """
        table_schema = self.get_table(table)
        if table_schema is None or not table_schema.columns:
            return None

        return random.choice(table_schema.columns)

    def get_column(self, table: str, column_name: str) -> Optional[ColumnSchema]:
        """
        Get a specific column from the specified table.

        Args:
            table: Table name
            column_name: Column name

        Returns:
            Column definition, or None if table or column does not exist
        """
        table_schema = self.get_table(table)
        if table_schema is None:
            return None

        for column in table_schema.columns:
            if column.name == column_name:
                return column

        return None

    def generate_value(self, column: ColumnSchema) -> Any:
        """
        Generate mock value for the specified column.

        Generates reasonable mock values based on the column's data type, used for SQL statement construction.

        Args:
            column: Column definition

        Returns:
            Mock value matching the column's type

        Examples:
            - int type returns random integer
            - str type returns string
            - float type returns random float
        """
        column_type = column.type

        # Generate corresponding value based on type
        if column_type == int:
            return random.randint(1, 1000)
        elif column_type == str:
            # Generate random string
            length = random.randint(5, 20)
            return "".join(random.choices("abcdefghijklmnopqrstuvwxyz", k=length))
        elif column_type == float:
            return round(random.uniform(1.0, 1000.0), 2)
        elif column_type == bool:
            return random.choice([True, False])
        else:
            # Default to string representation
            return f"value_of_type_{column_type.__name__}"
