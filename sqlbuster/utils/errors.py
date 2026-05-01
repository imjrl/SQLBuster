"""
Custom Exception Classes Module

This module defines all custom exception classes used in the SQLBuster project,
provides clear error categorization and context information for easy error handling and debugging.
"""


class SQLBusterError(Exception):
    """
    SQLBuster Base Exception Class

    Base class for all SQLBuster custom exceptions, provides unified exception interface.
    """

    def __init__(self, message: str = "Unknown error occurred in SQLBuster") -> None:
        """
        Initialize base exception

        Args:
            message: Error message
        """
        self.message = message
        super().__init__(self.message)


class SchemaError(SQLBusterError):
    """
    Schema Registry Related Exceptions

    Thrown when operations on schema registry (e.g. registering tables, querying column info) fail.
    """

    def __init__(self, message: str = "Schema registry operation failed") -> None:
        """
        初始化模式异常

        Args:
            message: Error message
        """
        super().__init__(message)


class TableNotFoundError(SchemaError):
    """
    Table Not Found Exception

    Thrown when trying to access an unregistered table.
    """

    def __init__(self, table_name: str) -> None:
        """
        初始化Table Not Found Exception

        Args:
            table_name: 未找到的Table name
        """
        message = f"表 '{table_name}' 未在模式注册表中找到"
        super().__init__(message)


class ColumnNotFoundError(SchemaError):
    """
    Column Not Found Exception

    Thrown when trying to access a non-existent column in a table.
    """

    def __init__(self, column_name: str, table_name: str) -> None:
        """
        初始化Column Not Found Exception

        Args:
            column_name: 未找到的Column name
            table_name: Table name
        """
        message = f"列 '{column_name}' 在表 '{table_name}' 中未找到"
        super().__init__(message)


class SQLExecutionError(SQLBusterError):
    """
    SQL Execution Related Exceptions

    Thrown when an error occurs during SQL execution (not when database returns False).
    """

    def __init__(self, sql: str, reason: str = "Execution failed") -> None:
        """
        初始化SQL执行异常

        Args:
            sql: Executed SQL statement
            reason: Failure reason
        """
        message = f"SQL execution error: {reason}\nSQL: {sql}"
        self.sql = sql
        self.reason = reason
        super().__init__(message)


class ClauseGenerationError(SQLBusterError):
    """
    Clause Generation Related Exceptions

    Thrown when an error occurs during SQL clause variant generation.
    """

    def __init__(self, clause_type: str, reason: str = "Generation failed") -> None:
        """
        初始化子句生成异常

        Args:
            clause_type: Clause type
            reason: Failure reason
        """
        message = f"子句 '{clause_type}' Generation failed: {reason}"
        super().__init__(message)


class EngineError(SQLBusterError):
    """
    Evolution Engine Related Exceptions

    Thrown when an error occurs during evolution engine operation.
    """

    def __init__(self, message: str = "Evolution engine runtime error") -> None:
        """
        初始化Engine异常

        Args:
            message: Error message
        """
        super().__init__(message)


class ReporterError(SQLBusterError):
    """
    Report Generation Related Exceptions

    Thrown when an error occurs during report generation or saving.
    """

    def __init__(self, message: str = "Report generation error") -> None:
        """
        初始化报告异常

        Args:
            message: Error message
        """
        super().__init__(message)
