"""
SQL执行器单元测试

本模块测试sqlbuster.core.runner中的BaseSQLRunner、MockRunner和DatabaseRunner类。
"""

import unittest
from typing import Set

from sqlbuster.core.runner import BaseSQLRunner, DatabaseRunner, MockRunner


class TestMockRunner(unittest.TestCase):
    """测试MockRunner类"""

    def test_mock_runner_initialization(self) -> None:
        """测试MockRunner初始化"""
        runner = MockRunner()
        self.assertIsNotNone(runner)
        self.assertEqual(len(runner.list_unsupported_clauses()), 0)

    def test_mock_runner_with_unsupported_clauses(self) -> None:
        """测试带不支持子句的MockRunner初始化"""
        unsupported: Set[str] = {"HAVING", "LIMIT"}
        runner = MockRunner(unsupported)
        self.assertEqual(runner.list_unsupported_clauses(), {"HAVING", "LIMIT"})

    def test_execute_supported_sql(self) -> None:
        """测试执行支持的SQL"""
        runner = MockRunner()
        result = runner.execute("SELECT * FROM users")
        self.assertTrue(result)

    def test_execute_unsupported_sql(self) -> None:
        """测试执行不支持的SQL"""
        runner = MockRunner({"HAVING"})
        result = runner.execute("SELECT * FROM users HAVING COUNT(*) > 1")
        self.assertFalse(result)

    def test_execute_sql_with_multiple_clauses(self) -> None:
        """测试执行包含多个子句的SQL"""
        runner = MockRunner({"LIMIT"})
        sql = "SELECT * FROM users WHERE age > 18 ORDER BY name"
        result = runner.execute(sql)
        self.assertTrue(result)  # LIMIT未出现，应该成功

        sql_with_limit = "SELECT * FROM users WHERE age > 18 LIMIT 10"
        result = runner.execute(sql_with_limit)
        self.assertFalse(result)  # 包含LIMIT，应该失败

    def test_add_unsupported_clause(self) -> None:
        """测试添加不支持的子句"""
        runner = MockRunner()
        runner.add_unsupported_clause("HAVING")
        self.assertIn("HAVING", runner.list_unsupported_clauses())

    def test_remove_unsupported_clause(self) -> None:
        """测试移除不支持的子句"""
        runner = MockRunner({"HAVING", "LIMIT"})
        runner.remove_unsupported_clause("HAVING")
        self.assertNotIn("HAVING", runner.list_unsupported_clauses())
        self.assertIn("LIMIT", runner.list_unsupported_clauses())

    def test_case_insensitive_matching(self) -> None:
        """测试不区分大小写的匹配"""
        runner = MockRunner({"HAVING"})
        # 小写having也应该被拒绝
        result = runner.execute("SELECT * FROM users having count(*) > 1")
        self.assertFalse(result)

    def test_word_boundary_matching(self) -> None:
        """测试单词边界匹配"""
        runner = MockRunner({"HAVING"})
        # 不应该匹配到"HAVINGS"或其他包含HAVING的字符串
        result = runner.execute("SELECT * FROM users WHERE HAVINGS = 1")
        self.assertTrue(result)  # HAVINGS不是HAVING，应该成功

    def test_execute_empty_sql(self) -> None:
        """测试执行空SQL"""
        runner = MockRunner()
        result = runner.execute("")
        self.assertTrue(result)  # 空SQL不包含任何不支持的子句

    def test_validate_syntax_default(self) -> None:
        """测试默认的语法验证方法"""
        runner = MockRunner()
        result = runner.validate_syntax("SELECT * FROM users")
        self.assertTrue(result)  # 默认返回True


class TestDatabaseRunner(unittest.TestCase):
    """测试DatabaseRunner类"""

    def test_database_runner_initialization(self) -> None:
        """测试DatabaseRunner初始化"""
        # 使用mock连接对象
        mock_connection = object()
        runner = DatabaseRunner(mock_connection)
        self.assertIsNotNone(runner)
        self.assertEqual(runner.connection, mock_connection)

    def test_execute_no_connection(self) -> None:
        """测试无连接时执行"""
        runner = DatabaseRunner(None)
        result = runner.execute("SELECT 1")
        self.assertFalse(result)

    def test_close_connection(self) -> None:
        """测试关闭连接"""
        mock_connection = object()
        runner = DatabaseRunner(mock_connection)
        # 应该不抛出异常
        runner.close()
        self.assertIsNone(runner.connection)


class TestBaseSQLRunner(unittest.TestCase):
    """测试BaseSQLRunner抽象类"""

    def test_cannot_instantiate_abstract_class(self) -> None:
        """测试不能实例化抽象类"""
        with self.assertRaises(TypeError):
            runner = BaseSQLRunner()  # type: ignore

    def test_concrete_implementation(self) -> None:
        """测试具体实现"""

        class TestRunner(BaseSQLRunner):
            def execute(self, sql: str) -> bool:
                return True

        runner = TestRunner()
        self.assertTrue(runner.execute("SELECT 1"))


if __name__ == "__main__":
    unittest.main()
