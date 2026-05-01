"""
SQL Function Registry Unit Tests

This module tests SQL function registry related classes in sqlbuster.core.function_registry,
Including SQLFunction, SQLFunctionParameter, and FunctionRegistry.
"""

import unittest
from typing import Any, Dict, List, Optional

from sqlbuster.core.function_registry import (
    FunctionRegistry,
    SQLFunction,
    SQLFunctionParameter,
)


class TestSQLFunctionParameter(unittest.TestCase):
    """Test SQLFunctionParameter class"""

    def test_create_basic_parameter(self) -> None:
        """Test creating basic parameter"""
        param = SQLFunctionParameter(name="expr", param_type="any")
        self.assertEqual(param.name, "expr")
        self.assertEqual(param.param_type, "any")
        self.assertFalse(param.is_optional)
        self.assertFalse(param.is_variadic)
        self.assertIsNone(param.default_value)

    def test_create_optional_parameter(self) -> None:
        """Test creating optional parameter"""
        param = SQLFunctionParameter(
            name="length",
            param_type="integer",
            is_optional=True,
            default_value=None,
        )
        self.assertTrue(param.is_optional)
        self.assertIsNone(param.default_value)

    def test_create_variadic_parameter(self) -> None:
        """Test creating variadic parameter"""
        param = SQLFunctionParameter(name="values", param_type="any", is_variadic=True)
        self.assertTrue(param.is_variadic)


class TestSQLFunction(unittest.TestCase):
    """Test SQLFunction class"""

    def test_create_basic_function(self) -> None:
        """Test creating basic function"""
        func = SQLFunction(
            name="UPPER",
            return_type="string",
            parameters=[
                SQLFunctionParameter(name="string", param_type="string"),
            ],
            sql_template="UPPER({string})",
            category="string",
        )
        self.assertEqual(func.name, "UPPER")
        self.assertEqual(func.return_type, "string")
        self.assertEqual(len(func.parameters), 1)
        self.assertFalse(func.is_aggregate)
        self.assertFalse(func.supports_distinct)

    def test_create_aggregate_function(self) -> None:
        """Test creating aggregate function"""
        func = SQLFunction(
            name="COUNT",
            return_type="integer",
            parameters=[
                SQLFunctionParameter(name="expr", param_type="any"),
            ],
            sql_template="COUNT({expr})",
            category="aggregate",
            is_aggregate=True,
            supports_distinct=True,
        )
        self.assertTrue(func.is_aggregate)
        self.assertTrue(func.supports_distinct)


class TestFunctionRegistry(unittest.TestCase):
    """Test FunctionRegistry class"""

    def setUp(self) -> None:
        """Setup test environment"""
        self.registry = FunctionRegistry()

    def test_initialization_registers_sql92_functions(self) -> None:
        """Test registering SQL-92 functions during initialization"""
        # Check aggregate functions
        count_func = self.registry.get_function("COUNT")
        self.assertIsNotNone(count_func)
        self.assertTrue(count_func.is_aggregate)

        sum_func = self.registry.get_function("SUM")
        self.assertIsNotNone(sum_func)
        self.assertTrue(sum_func.is_aggregate)

        # Check string functions
        upper_func = self.registry.get_function("UPPER")
        self.assertIsNotNone(upper_func)
        self.assertEqual(upper_func.category, "string")

        # Check numeric functions
        abs_func = self.registry.get_function("ABS")
        self.assertIsNotNone(abs_func)
        self.assertEqual(abs_func.category, "numeric")

    def test_get_function_exists(self) -> None:
        """Test getting existing function"""
        func = self.registry.get_function("COUNT")
        self.assertIsNotNone(func)
        self.assertEqual(func.name, "COUNT")

    def test_get_function_not_exists(self) -> None:
        """Test getting non-existent function"""
        func = self.registry.get_function("NONEXISTENT")
        self.assertIsNone(func)

    def test_get_functions_by_category(self) -> None:
        """Test getting functions by category"""
        aggregate_funcs = self.registry.get_functions_by_category("aggregate")
        self.assertGreater(len(aggregate_funcs), 0)

        string_funcs = self.registry.get_functions_by_category("string")
        self.assertGreater(len(string_funcs), 0)

        numeric_funcs = self.registry.get_functions_by_category("numeric")
        self.assertGreater(len(numeric_funcs), 0)

    def test_get_all_functions(self) -> None:
        """Test getting all functions"""
        all_funcs = self.registry.get_all_functions()
        self.assertGreater(len(all_funcs), 0)

    def test_get_all_categories(self) -> None:
        """Test getting all categories"""
        categories = self.registry.get_all_categories()
        self.assertIn("aggregate", categories)
        self.assertIn("string", categories)
        self.assertIn("numeric", categories)
        self.assertIn("datetime", categories)
        self.assertIn("conversion", categories)
        self.assertIn("conditional", categories)

    def test_register_custom_function(self) -> None:
        """Test registering custom function"""
        self.registry.register_custom_function(
            name="MY_FUNC",
            return_type="string",
            parameters=[
                {"name": "input", "type": "string"},
            ],
            sql_template="MY_FUNC({input})",
            description="My custom function",
        )

        func = self.registry.get_function("MY_FUNC")
        self.assertIsNotNone(func)
        self.assertEqual(func.category, "custom")
        self.assertEqual(func.description, "My custom function")

    def test_register_duplicate_function_raises(self) -> None:
        """Test duplicate function registration raises error"""
        with self.assertRaises(ValueError):
            self.registry.register_function(
                SQLFunction(
                    name="COUNT",
                    return_type="integer",
                    parameters=[],
                    sql_template="COUNT(*)",
                )
            )

    def test_generate_sql_simple(self) -> None:
        """Test generating simple function SQL"""
        sql = self.registry.generate_sql("UPPER", {"string": "name"})
        self.assertEqual(sql, "UPPER(name)")

    def test_generate_sql_aggregate(self) -> None:
        """Test generating aggregate function SQL"""
        sql = self.registry.generate_sql("COUNT", {"expr": "age"})
        self.assertEqual(sql, "COUNT(age)")

    def test_generate_sql_with_distinct(self) -> None:
        """Test generating aggregate function SQL with DISTINCT"""
        sql = self.registry.generate_sql("COUNT", {"expr": "age"}, use_distinct=True)
        self.assertEqual(sql, "COUNT(DISTINCT age)")

    def test_generate_sql_function_not_exists(self) -> None:
        """Test generating SQL for non-existent function"""
        sql = self.registry.generate_sql("NONEXISTENT", {})
        self.assertIsNone(sql)

    def test_generate_sql_missing_required_param(self) -> None:
        """Test generating SQL when missing required parameters"""
        # COUNT needs expr parameter, should return None if not provided
        sql = self.registry.generate_sql("COUNT", {})
        self.assertIsNone(sql)

    def test_generate_sql_optional_param(self) -> None:
        """Test generating SQL for function with optional parameters"""
        # ROUND function has optional decimal places parameter
        sql = self.registry.generate_sql("ROUND", {"numeric": "amount"})
        self.assertEqual(sql, "ROUND(amount)")

        # Provide optional parameter
        sql = self.registry.generate_sql("ROUND", {"numeric": "amount", "decimals": 2})
        self.assertEqual(sql, "ROUND(amount, 2)")

    def test_generate_sql_variadic_param(self) -> None:
        """Test generating SQL for function with variadic parameters"""
        # COALESCE function has variadic parameters
        sql = self.registry.generate_sql(
            "COALESCE", {"values": ["col1", "col2", "'default'"]}
        )
        self.assertIn("COALESCE", sql)
        self.assertIn("col1", sql)

    def test_validate_function_call_valid(self) -> None:
        """Test validating valid function call"""
        is_valid, error_msg = self.registry.validate_function_call(
            "UPPER", {"string": "name"}
        )
        self.assertTrue(is_valid)
        self.assertIsNone(error_msg)

    def test_validate_function_call_invalid_missing_param(self) -> None:
        """Test validating invalid function call (missing parameters)"""
        is_valid, error_msg = self.registry.validate_function_call(
            "UPPER", {}  # 缺少必需参数string
        )
        self.assertFalse(is_valid)
        self.assertIsNotNone(error_msg)

    def test_validate_function_call_function_not_exists(self) -> None:
        """Test validating non-existent function call"""
        is_valid, error_msg = self.registry.validate_function_call("NONEXISTENT", {})
        self.assertFalse(is_valid)
        self.assertIn("不存在", error_msg)

    def test_list_functions(self) -> None:
        """Test listing all functions"""
        result = self.registry.list_functions()
        self.assertIn("已注册的SQL函数", result)
        self.assertIn("COUNT", result)
        self.assertIn("UPPER", result)


class TestFunctionRegistryIntegration(unittest.TestCase):
    """Test FunctionRegistry integration function"""

    def test_custom_function_in_select_clause(self) -> None:
        """Test using custom functions in SELECT clause"""
        registry = FunctionRegistry()

        # Register custom function
        registry.register_custom_function(
            name="DOUBLE_VALUE",
            return_type="numeric",
            parameters=[{"name": "value", "type": "numeric"}],
            sql_template="({value} * 2)",
        )

        # Generate SQL
        sql = registry.generate_sql("DOUBLE_VALUE", {"value": "price"})
        self.assertEqual(sql, "(price * 2)")

    def test_multiple_custom_functions(self) -> None:
        """Test registering multiple custom functions"""
        registry = FunctionRegistry()

        # Register multiple custom functions
        registry.register_custom_function(
            name="ADD_ONE",
            return_type="numeric",
            parameters=[{"name": "x", "type": "numeric"}],
            sql_template="({x} + 1)",
        )

        registry.register_custom_function(
            name="SQUARE",
            return_type="numeric",
            parameters=[{"name": "x", "type": "numeric"}],
            sql_template="({x} * {x})",
        )

        # Verify both functions registered successfully
        self.assertIsNotNone(registry.get_function("ADD_ONE"))
        self.assertIsNotNone(registry.get_function("SQUARE"))

        # Generate SQL
        sql1 = registry.generate_sql("ADD_ONE", {"x": "5"})
        self.assertEqual(sql1, "(5 + 1)")

        sql2 = registry.generate_sql("SQUARE", {"x": "3"})
        self.assertEqual(sql2, "(3 * 3)")


if __name__ == "__main__":
    unittest.main()
