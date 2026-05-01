"""
SQL Runner Module

This module defines the abstract base class and mock implementation for SQL runners,
providing a unified SQL execution interface that supports real databases and mock environments.
"""

import re
from abc import ABC, abstractmethod
from typing import Any, Optional, Set


class BaseSQLRunner(ABC):
    """
    Abstract Base Class for SQL Runners

    Defines a unified interface for SQL execution. All concrete runners must inherit from this class and implement the execute method.
    Supports optional connection objects and syntax validation.
    """

    def __init__(self, connection: Optional[Any] = None) -> None:
        """
        Initialize the runner

        Args:
            connection: Optional database connection object, specific type depends on subclass implementation
        """
        self.connection = connection

    @abstractmethod
    def execute(self, sql: str) -> bool:
        """
        Execute SQL statement

        Executes the SQL statement in the target database or mock environment, returns execution result.

        Args:
            sql: SQL statement to execute

        Returns:
            True: Execution succeeded
            False: Execution failed (database error or syntax error)
        """
        pass

    def validate_syntax(self, sql: str) -> bool:
        """
        Validate SQL syntax

        Optional method to validate the syntax correctness of SQL statements before execution.
        Default implementation returns True; subclasses can override this method to provide custom validation logic.

        Args:
            sql: SQL statement to validate

        Returns:
            Whether the syntax is correct
        """
        return True


class MockRunner(BaseSQLRunner):
    """
    Mock Runner

    Mock SQL runner for testing, determines whether SQL is executable based on a preset set of unsupported clauses.
    Simulates database behavior that does not support certain SQL features.

    Supported clause types include:
    - Basic clauses: SELECT, WHERE, GROUP BY, HAVING, ORDER BY, LIMIT
    - Newly supported clauses: WITH (CTE), JOIN, WINDOW, UNION

    You can simulate different database capabilities by adding unsupported clauses via add_unsupported_clause().
    """

    def __init__(self, unsupported_clauses: Optional[Set[str]] = None) -> None:
        """
        Initialize mock runner

        Args:
            unsupported_clauses: Set of unsupported SQL clause keywords
                            (e.g., {'HAVING', 'LIMIT'} means HAVING and LIMIT clauses are not supported)
        """
        super().__init__(connection=None)
        self._unsupported_clauses: Set[str] = (
            unsupported_clauses.copy() if unsupported_clauses else set()
        )

    def execute(self, sql: str) -> bool:
        """
        Mock execute SQL

        Checks whether the SQL statement contains any unsupported clause keywords.
        Returns True if no unsupported clauses are present (simulated execution success);
        otherwise returns False (simulated execution failure).

        Args:
            sql: SQL statement to execute

        Returns:
            True: SQL contains no unsupported clauses, simulated execution succeeded
            False: SQL contains unsupported clauses, simulated execution failed
        """
        sql_upper = sql.upper()

        for clause in self._unsupported_clauses:
            # Use regex to ensure full clause keyword matching
            # For example, avoid matching "HAVING" to "HAVINGS", etc.
            pattern = r"\b" + re.escape(clause.upper()) + r"\b"
            if re.search(pattern, sql_upper):
                return False

        return True

    def add_unsupported_clause(self, clause: str) -> None:
        """
        Add unsupported SQL clause

        Adds the specified SQL clause keyword to the unsupported list,
        after which all SQL containing this clause will simulate execution failure.

        Args:
            clause: Clause keyword (e.g., 'HAVING', 'LIMIT', 'WITH', 'JOIN', 'WINDOW', 'UNION', etc.)
                   Case-insensitive, stored in uppercase

        Examples:
            runner.add_unsupported_clause('WITH')  # CTE not supported
            runner.add_unsupported_clause('JOIN')  # JOIN not supported
            runner.add_unsupported_clause('WINDOW')  # WINDOW not supported
            runner.add_unsupported_clause('UNION')  # UNION not supported
        """
        self._unsupported_clauses.add(clause.upper())

    def remove_unsupported_clause(self, clause: str) -> None:
        """
        Remove unsupported SQL clause

        Removes the specified SQL clause keyword from the unsupported list,
        after which the clause can be executed normally (simulated).

        Args:
            clause: Clause keyword to remove
        """
        clause_upper = clause.upper()
        if clause_upper in self._unsupported_clauses:
            self._unsupported_clauses.remove(clause_upper)

    def list_unsupported_clauses(self) -> Set[str]:
        """
        List all unsupported SQL clauses

        Returns:
            Set of unsupported clause keywords
        """
        return self._unsupported_clauses.copy()


class DatabaseRunner(BaseSQLRunner):
    """
    Real Database Runner

    Used to execute SQL statements on real databases.
    Requires a valid database connection object to be passed in.
    """

    def __init__(self, connection: Any) -> None:
        """
        Initialize database runner

        Args:
            connection: Database connection object
                       For SQLite: sqlite3.Connection
                       For MySQL: pymysql.Connection or mysql.connector.connection
                       For PostgreSQL: psycopg2.extensions.connection
        """
        super().__init__(connection=connection)

    def execute(self, sql: str) -> bool:
        """
        Execute SQL on real database

        Executes SQL statements using the internal database connection.
        Returns True if execution succeeds, False if any exception occurs.

        Args:
            sql: SQL statement to execute

        Returns:
            True: Execution succeeded
            False: Execution failed (exception occurred)
        """
        if self.connection is None:
            return False

        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(sql)

            # For SELECT queries, fetch results to avoid unread result issues
            if sql.strip().upper().startswith("SELECT"):
                cursor.fetchall()

            # For non-autocommit connections, commit is required
            if hasattr(self.connection, "commit"):
                self.connection.commit()

            return True
        except Exception:
            # 发生任何异常都返回False
            # In production environments, logging should be added here
            return False
        finally:
            if cursor:
                cursor.close()

    def close(self) -> None:
        """
        Close database connection

        Closes the internal database connection and releases resources.
        Silently handles cases where the connection is already closed or does not exist.
        Sets connection to None regardless of whether an exception occurs.
        """
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception:
                # Ignore exceptions during close
                pass
            finally:
                # Ensure connection is set to None
                self.connection = None
