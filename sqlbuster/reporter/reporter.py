"""
报告生成模块

本模块实现了探索结果报告生成器，负责收集、分析和展示
SQLBuster进化Engine的探索结果，支持多种输出格式。
"""

import json
import re
from collections import defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional

from sqlbuster.core.engine import ClauseType, SQLNode


class Reporter:
    """
    探索结果报告生成器

    收集SQLNode执行结果，生成树形结构报告、JSON格式报告，
    并提供统计信息分析。支持将报告保存到文件。
    """

    def __init__(self) -> None:
        """初始化报告器"""
        self._root_nodes: List[SQLNode] = []
        self._execution_log: List[Dict[str, Any]] = []
        self._start_time: Optional[datetime] = None
        self._end_time: Optional[datetime] = None

    def add_root_node(self, node: SQLNode) -> None:
        """
        添加根节点

        Args:
            node: 要添加的根节点
        """
        self._root_nodes.append(node)

    def add_execution_result(self, node: SQLNode) -> None:
        """
        添加执行结果到报告

        记录单个节点的执行结果，并自动收集根节点。

        Args:
            node: 执行节点
        """
        # 如果还没有记录开始时间
        if self._start_time is None:
            self._start_time = datetime.now()

        # 记录执行日志
        log_entry = {
            "sql": node.sql,
            "clause_type": node.clause_type.value if node.clause_type else None,
            "success": node.success,
            "error_msg": node.error_msg,
            "timestamp": datetime.now().isoformat(),
            "is_root": node.is_root(),
            "is_leaf": node.is_leaf(),
        }
        self._execution_log.append(log_entry)

        # 如果是根节点且尚未记录，添加到根节点列表
        if node.is_root() and node not in self._root_nodes:
            self._root_nodes.append(node)

    def _node_to_dict(self, node: SQLNode) -> Dict[str, Any]:
        """
        将SQLNode转换为字典表示

        Args:
            node: SQL节点

        Returns:
            节点的字典表示
        """
        result = {
            "sql": node.sql,
            "clause_type": node.clause_type.value if node.clause_type else None,
            "success": node.success,
            "error_msg": node.error_msg,
            "children": [self._node_to_dict(child) for child in node.children],
        }
        return result

    def generate_tree_report(self) -> Dict[str, Any]:
        """
        生成树形结构报告

        将所有根节点及其子节点转换为树形字典结构。

        Returns:
            树形结构字典，包含根节点列表和元数据
        """
        self._end_time = datetime.now()

        report = {
            "metadata": {
                "start_time": (
                    self._start_time.isoformat() if self._start_time else None
                ),
                "end_time": self._end_time.isoformat() if self._end_time else None,
                "total_nodes": len(self._execution_log),
                "root_nodes_count": len(self._root_nodes),
            },
            "roots": [self._node_to_dict(root) for root in self._root_nodes],
        }

        return report

    def generate_json_report(self) -> str:
        """
        生成JSON格式报告

        将树形结构报告转换为格式化的JSON字符串。

        Returns:
            JSON字符串
        """
        tree_report = self.generate_tree_report()

        return json.dumps(tree_report, indent=2, ensure_ascii=False)

    def save_report(self, path: str, format: str = "json") -> None:
        """
        保存报告到文件

        Args:
            path: 文件路径
            format: 报告格式 ('json' 或 'tree')

        Raises:
            ValueError: 如果格式不支持
        """
        format = format.lower()

        if format == "json":
            content = self.generate_json_report()
        elif format == "tree":
            content = self._generate_text_tree()
        else:
            raise ValueError(f"不支持的报告格式: {format}，请使用 'json' 或 'tree'")

        with open(path, "w", encoding="utf-8") as f:
            f.write(content)

    def _generate_text_tree(self) -> str:
        """
        生成文本树形报告

        Returns:
            文本格式的树形报告
        """
        lines = []
        lines.append("SQLBuster 探索报告")
        lines.append("=" * 50)

        stats = self.get_statistics()
        lines.append(f"Total executions: {stats['total_executions']}")
        lines.append(f"Successful executions: {stats['successful_executions']}")
        lines.append(f"Failed executions: {stats['failed_executions']}")
        lines.append(f"Success rate: {stats['success_rate']:.2%}")
        lines.append("")

        for root in self._root_nodes:
            lines.append("根节点:")
            self._append_node_text(root, lines, prefix="  ")

        return "\n".join(lines)

    def _append_node_text(
        self, node: SQLNode, lines: List[str], prefix: str = ""
    ) -> None:
        """
        递归添加节点文本

        Args:
            node: 当前节点
            lines: 文本行列表
            prefix: 当前前缀
        """
        # 构建节点描述
        status = "✓" if node.success else "✗" if node.success is not None else "?"
        clause_info = f" [{node.clause_type.value}]" if node.clause_type else ""
        lines.append(f"{prefix}{status}{clause_info} {node.sql[:80]}")

        # 递归处理子节点
        child_prefix = prefix + "  "
        for child in node.children:
            self._append_node_text(child, lines, child_prefix)

    def get_statistics(self) -> Dict[str, Any]:
        """
        获取探索统计信息

        分析执行日志，生成详细的统计信息。

        Returns:
            统计信息字典，包含执行次数、成功率、按Clause type分组等
        """
        total = len(self._execution_log)
        successful = sum(1 for log in self._execution_log if log["success"] is True)
        failed = sum(1 for log in self._execution_log if log["success"] is False)

        # 按Clause type分组统计
        clause_stats: Dict[str, Dict[str, int]] = defaultdict(
            lambda: {"total": 0, "success": 0, "failed": 0}
        )

        for log in self._execution_log:
            clause_type = log["clause_type"] or "ROOT"
            clause_stats[clause_type]["total"] += 1
            if log["success"] is True:
                clause_stats[clause_type]["success"] += 1
            elif log["success"] is False:
                clause_stats[clause_type]["failed"] += 1

        # 计算成功率
        success_rate = successful / total if total > 0 else 0.0

        return {
            "total_executions": total,
            "successful_executions": successful,
            "failed_executions": failed,
            "success_rate": success_rate,
            "clause_type_statistics": dict(clause_stats),
            "execution_log": self._execution_log,
        }

    def generate_summary(self) -> str:
        """
        生成简洁的摘要报告

        Returns:
            文本格式的摘要
        """
        stats = self.get_statistics()

        lines = [
            "SQLBuster 探索摘要",
            "=" * 30,
            f"Total executions: {stats['total_executions']}",
            f"成功: {stats['successful_executions']}",
            f"失败: {stats['failed_executions']}",
            f"Success rate: {stats['success_rate']:.2%}",
            "",
            "Statistics by clause type:",
        ]

        for clause_type, stat in stats["clause_type_statistics"].items():
            rate = stat["success"] / stat["total"] if stat["total"] > 0 else 0
            lines.append(
                f"  {clause_type}: {stat['success']}/{stat['total']} " f"({rate:.2%})"
            )

        return "\n".join(lines)
