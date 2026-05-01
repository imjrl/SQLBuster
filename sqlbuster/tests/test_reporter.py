"""
报告生成器单元测试

本模块测试sqlbuster.reporter中的报告生成功能，
重点测试Reporter类的初始化、记录执行结果、生成报告及统计信息计算。
"""

import unittest
from datetime import datetime
from typing import Any, Dict, List

from sqlbuster.core.engine import ClauseType, SQLNode
from sqlbuster.reporter import Reporter


class TestReporterInitialization(unittest.TestCase):
    """测试Reporter类初始化"""

    def test_reporter_creation(self) -> None:
        """测试创建Reporter实例"""
        reporter = Reporter()
        self.assertIsNotNone(reporter)
        self.assertEqual(len(reporter._root_nodes), 0)
        self.assertEqual(len(reporter._execution_log), 0)
        self.assertIsNone(reporter._start_time)
        self.assertIsNone(reporter._end_time)

    def test_reporter_add_root_node(self) -> None:
        """测试添加根节点"""
        reporter = Reporter()
        node = SQLNode(sql="SELECT * FROM users")
        reporter.add_root_node(node)

        self.assertEqual(len(reporter._root_nodes), 1)
        self.assertEqual(reporter._root_nodes[0], node)


class TestReporterAddExecutionResult(unittest.TestCase):
    """测试Reporter记录执行结果功能"""

    def setUp(self) -> None:
        """测试前准备"""
        self.reporter = Reporter()

    def test_add_successful_execution(self) -> None:
        """测试记录成功的执行结果"""
        # 创建一个根节点作为父节点
        root = SQLNode(sql="SELECT * FROM users", success=True)
        node = SQLNode(
            sql="SELECT * FROM users WHERE age > 18",
            clause_type=ClauseType.WHERE,
            success=True,
            parent=root,
        )
        self.reporter.add_execution_result(node)

        self.assertEqual(len(self.reporter._execution_log), 1)
        log = self.reporter._execution_log[0]
        self.assertEqual(log["sql"], "SELECT * FROM users WHERE age > 18")
        self.assertEqual(log["clause_type"], "WHERE")
        self.assertTrue(log["success"])
        self.assertIsNone(log["error_msg"])
        self.assertFalse(log["is_root"])
        self.assertTrue(log["is_leaf"])

    def test_add_failed_execution(self) -> None:
        """测试记录失败的执行结果"""
        node = SQLNode(
            sql="SELECT * FROM non_existent_table",
            clause_type=None,
            success=False,
            error_msg="Table not found",
        )
        self.reporter.add_execution_result(node)

        self.assertEqual(len(self.reporter._execution_log), 1)
        log = self.reporter._execution_log[0]
        self.assertFalse(log["success"])
        self.assertEqual(log["error_msg"], "Table not found")
        self.assertTrue(log["is_root"])

    def test_add_root_node_automatically(self) -> None:
        """测试自动添加根节点"""
        node = SQLNode(sql="SELECT * FROM users", success=True)
        self.reporter.add_execution_result(node)

        # 根节点应该被自动添加到_root_nodes
        self.assertEqual(len(self.reporter._root_nodes), 1)
        self.assertEqual(self.reporter._root_nodes[0], node)

    def test_add_child_node_not_added_to_roots(self) -> None:
        """测试子节点不会被添加到根节点列表"""
        root = SQLNode(sql="SELECT * FROM users", success=True)
        child = SQLNode(
            sql="SELECT * FROM users WHERE age > 18",
            clause_type=ClauseType.WHERE,
            success=True,
            parent=root,
        )

        self.reporter.add_execution_result(root)
        self.reporter.add_execution_result(child)

        # 只有根节点应该在_root_nodes中
        self.assertEqual(len(self.reporter._root_nodes), 1)
        self.assertEqual(self.reporter._root_nodes[0], root)

    def test_start_time_recorded(self) -> None:
        """测试开始时间被记录"""
        self.assertIsNone(self.reporter._start_time)

        node = SQLNode(sql="SELECT 1", success=True)
        self.reporter.add_execution_result(node)

        self.assertIsNotNone(self.reporter._start_time)
        self.assertIsInstance(self.reporter._start_time, datetime)


class TestReporterGenerateTreeReport(unittest.TestCase):
    """测试Reporter生成树形报告功能"""

    def setUp(self) -> None:
        """测试前准备"""
        self.reporter = Reporter()

    def test_generate_empty_report(self) -> None:
        """测试生成空报告"""
        report = self.reporter.generate_tree_report()

        self.assertIsNotNone(report["metadata"]["end_time"])
        self.assertEqual(report["metadata"]["total_nodes"], 0)
        self.assertEqual(report["metadata"]["root_nodes_count"], 0)
        self.assertEqual(len(report["roots"]), 0)

    def test_generate_report_with_single_node(self) -> None:
        """测试生成包含单个节点的报告"""
        node = SQLNode(sql="SELECT * FROM users", success=True)
        self.reporter.add_execution_result(node)

        report = self.reporter.generate_tree_report()

        self.assertEqual(report["metadata"]["total_nodes"], 1)
        self.assertEqual(report["metadata"]["root_nodes_count"], 1)
        self.assertEqual(len(report["roots"]), 1)

        root_dict = report["roots"][0]
        self.assertEqual(root_dict["sql"], "SELECT * FROM users")
        self.assertTrue(root_dict["success"])
        self.assertEqual(len(root_dict["children"]), 0)

    def test_generate_report_with_tree(self) -> None:
        """测试生成包含树形结构的报告"""
        root = SQLNode(sql="SELECT * FROM users", success=True)
        child1 = SQLNode(
            sql="SELECT * FROM users WHERE age > 18",
            clause_type=ClauseType.WHERE,
            success=True,
            parent=root,
        )
        child2 = SQLNode(
            sql="SELECT * FROM users WHERE age > 18 LIMIT 10",
            clause_type=ClauseType.LIMIT,
            success=True,
            parent=child1,
        )
        root.add_child(child1)
        child1.add_child(child2)

        self.reporter.add_execution_result(root)
        self.reporter.add_execution_result(child1)
        self.reporter.add_execution_result(child2)

        report = self.reporter.generate_tree_report()

        self.assertEqual(report["metadata"]["total_nodes"], 3)
        self.assertEqual(len(report["roots"]), 1)

        # 验证树形结构
        root_dict = report["roots"][0]
        self.assertEqual(len(root_dict["children"]), 1)
        self.assertEqual(len(root_dict["children"][0]["children"]), 1)


class TestReporterGenerateJsonReport(unittest.TestCase):
    """测试Reporter生成JSON报告功能"""

    def setUp(self) -> None:
        """测试前准备"""
        self.reporter = Reporter()

    def test_generate_json_report(self) -> None:
        """测试生成JSON格式报告"""
        node = SQLNode(sql="SELECT * FROM users", success=True)
        self.reporter.add_execution_result(node)

        json_report = self.reporter.generate_json_report()

        self.assertIsInstance(json_report, str)
        # 验证是有效的JSON
        import json

        parsed = json.loads(json_report)
        self.assertIn("metadata", parsed)
        self.assertIn("roots", parsed)

    def test_json_report_with_clause_type(self) -> None:
        """测试JSON报告包含子句类型信息"""
        node = SQLNode(
            sql="SELECT * FROM users WHERE age > 18",
            clause_type=ClauseType.WHERE,
            success=True,
        )
        self.reporter.add_execution_result(node)

        json_report = self.reporter.generate_json_report()
        import json

        parsed = json.loads(json_report)

        self.assertEqual(parsed["roots"][0]["clause_type"], "WHERE")


class TestReporterSaveReport(unittest.TestCase):
    """测试Reporter保存报告到文件功能"""

    def setUp(self) -> None:
        """测试前准备"""
        self.reporter = Reporter()
        self.test_file = "test_report_output.json"

    def tearDown(self) -> None:
        """测试后清理"""
        import os

        if os.path.exists(self.test_file):
            os.remove(self.test_file)

    def test_save_json_report(self) -> None:
        """测试保存JSON格式报告"""
        node = SQLNode(sql="SELECT 1", success=True)
        self.reporter.add_execution_result(node)

        self.reporter.save_report(self.test_file, format="json")

        import os

        self.assertTrue(os.path.exists(self.test_file))

        with open(self.test_file, "r", encoding="utf-8") as f:
            import json

            content = json.load(f)
            self.assertIn("metadata", content)

    def test_save_tree_report(self) -> None:
        """测试保存文本树形报告"""
        node = SQLNode(sql="SELECT 1", success=True)
        self.reporter.add_execution_result(node)

        self.reporter.save_report(self.test_file, format="tree")

        import os

        self.assertTrue(os.path.exists(self.test_file))

        with open(self.test_file, "r", encoding="utf-8") as f:
            content = f.read()
            self.assertIn("SQLBuster 探索报告", content)

    def test_save_unsupported_format(self) -> None:
        """测试保存不支持的格式"""
        with self.assertRaises(ValueError):
            self.reporter.save_report(self.test_file, format="xml")


class TestReporterGetStatistics(unittest.TestCase):
    """测试Reporter统计信息计算功能"""

    def setUp(self) -> None:
        """测试前准备"""
        self.reporter = Reporter()

    def test_empty_statistics(self) -> None:
        """测试空数据的统计信息"""
        stats = self.reporter.get_statistics()

        self.assertEqual(stats["total_executions"], 0)
        self.assertEqual(stats["successful_executions"], 0)
        self.assertEqual(stats["failed_executions"], 0)
        self.assertEqual(stats["success_rate"], 0.0)
        self.assertEqual(len(stats["clause_type_statistics"]), 0)

    def test_statistics_with_success_only(self) -> None:
        """测试只有成功执行的统计"""
        for i in range(5):
            node = SQLNode(sql=f"SELECT {i}", success=True)
            self.reporter.add_execution_result(node)

        stats = self.reporter.get_statistics()

        self.assertEqual(stats["total_executions"], 5)
        self.assertEqual(stats["successful_executions"], 5)
        self.assertEqual(stats["failed_executions"], 0)
        self.assertEqual(stats["success_rate"], 1.0)

    def test_statistics_with_mixed_results(self) -> None:
        """测试混合成功失败的统计"""
        # 3个成功，2个失败
        for i in range(3):
            node = SQLNode(sql=f"SELECT {i}", success=True)
            self.reporter.add_execution_result(node)

        for i in range(2):
            node = SQLNode(
                sql=f"SELECT * FROM table_{i}",
                success=False,
                error_msg="Table not found",
            )
            self.reporter.add_execution_result(node)

        stats = self.reporter.get_statistics()

        self.assertEqual(stats["total_executions"], 5)
        self.assertEqual(stats["successful_executions"], 3)
        self.assertEqual(stats["failed_executions"], 2)
        self.assertEqual(stats["success_rate"], 0.6)

    def test_statistics_by_clause_type(self) -> None:
        """测试按子句类型统计"""
        # 根节点
        root = SQLNode(sql="SELECT * FROM users", success=True)
        self.reporter.add_execution_result(root)

        # WHERE子句
        where_node = SQLNode(
            sql="SELECT * FROM users WHERE age > 18",
            clause_type=ClauseType.WHERE,
            success=True,
        )
        self.reporter.add_execution_result(where_node)

        # LIMIT子句（失败）
        limit_node = SQLNode(
            sql="SELECT * FROM users WHERE age > 18 LIMIT 10",
            clause_type=ClauseType.LIMIT,
            success=False,
            error_msg="LIMIT not supported",
        )
        self.reporter.add_execution_result(limit_node)

        stats = self.reporter.get_statistics()
        clause_stats = stats["clause_type_statistics"]

        # ROOT统计
        self.assertIn("ROOT", clause_stats)
        self.assertEqual(clause_stats["ROOT"]["total"], 1)
        self.assertEqual(clause_stats["ROOT"]["success"], 1)

        # WHERE统计
        self.assertIn("WHERE", clause_stats)
        self.assertEqual(clause_stats["WHERE"]["total"], 1)
        self.assertEqual(clause_stats["WHERE"]["success"], 1)

        # LIMIT统计
        self.assertIn("LIMIT", clause_stats)
        self.assertEqual(clause_stats["LIMIT"]["total"], 1)
        self.assertEqual(clause_stats["LIMIT"]["failed"], 1)


class TestReporterGenerateSummary(unittest.TestCase):
    """测试Reporter生成摘要功能"""

    def setUp(self) -> None:
        """测试前准备"""
        self.reporter = Reporter()

    def test_generate_empty_summary(self) -> None:
        """测试生成空摘要"""
        summary = self.reporter.generate_summary()

        self.assertIn("SQLBuster 探索摘要", summary)
        self.assertIn("总执行次数: 0", summary)
        self.assertIn("成功: 0", summary)
        self.assertIn("失败: 0", summary)
        self.assertIn("成功率: 0.00%", summary)

    def test_generate_summary_with_data(self) -> None:
        """测试生成有数据的摘要"""
        # 添加一些执行结果
        for i in range(3):
            node = SQLNode(sql=f"SELECT {i}", success=True)
            self.reporter.add_execution_result(node)

        node = SQLNode(sql="SELECT * FROM bad_table", success=False)
        self.reporter.add_execution_result(node)

        summary = self.reporter.generate_summary()

        self.assertIn("总执行次数: 4", summary)
        self.assertIn("成功: 3", summary)
        self.assertIn("失败: 1", summary)
        self.assertIn("成功率: 75.00%", summary)
        self.assertIn("按子句类型统计:", summary)


class TestReporterEdgeCases(unittest.TestCase):
    """测试Reporter边界情况"""

    def setUp(self) -> None:
        """测试前准备"""
        self.reporter = Reporter()

    def test_node_without_clause_type(self) -> None:
        """测试没有子句类型的节点"""
        node = SQLNode(sql="SELECT 1", success=True)
        self.reporter.add_execution_result(node)

        stats = self.reporter.get_statistics()
        clause_stats = stats["clause_type_statistics"]

        # 应该是ROOT类型
        self.assertIn("ROOT", clause_stats)

    def test_node_with_none_success(self) -> None:
        """测试未执行的节点"""
        node = SQLNode(sql="SELECT 1", success=None)
        self.reporter.add_execution_result(node)

        stats = self.reporter.get_statistics()

        # None不应该被计入成功或失败
        self.assertEqual(stats["successful_executions"], 0)
        self.assertEqual(stats["failed_executions"], 0)

    def test_node_with_empty_sql(self) -> None:
        """测试空SQL的节点"""
        node = SQLNode(sql="", success=False, error_msg="Empty SQL")
        self.reporter.add_execution_result(node)

        self.assertEqual(len(self.reporter._execution_log), 1)
        self.assertEqual(self.reporter._execution_log[0]["sql"], "")

    def test_add_duplicate_root_node(self) -> None:
        """测试重复添加根节点"""
        node = SQLNode(sql="SELECT 1", success=True)
        self.reporter.add_execution_result(node)
        self.reporter.add_execution_result(node)  # 再次添加

        # 根节点不应该重复添加
        self.assertEqual(len(self.reporter._root_nodes), 1)

    def test_generate_report_twice(self) -> None:
        """测试多次生成报告"""
        node = SQLNode(sql="SELECT 1", success=True)
        self.reporter.add_execution_result(node)

        report1 = self.reporter.generate_tree_report()
        report2 = self.reporter.generate_tree_report()

        # 两次报告应该一致
        self.assertEqual(
            report1["metadata"]["total_nodes"], report2["metadata"]["total_nodes"]
        )
        # 但结束时间可能不同，因为每次调用都会更新


if __name__ == "__main__":
    unittest.main()
