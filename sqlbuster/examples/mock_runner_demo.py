"""
MockRunner Example File

This example demonstrates how to use SQLBuster for SQL capability boundary exploration.
Simulates a database that does not support HAVING clause, and demonstrates the exploration process.
Demonstrates representative sampling optimization: when a column of a type succeeds in testing a clause,
other columns of the same type are no longer enumerated, reducing redundant tests.
"""

import logging
import os
import sys
from typing import Dict

# Add project root directory to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from sqlbuster.clauses.base import register_clause_generator
from sqlbuster.clauses.group_by import GroupByClauseGenerator
from sqlbuster.clauses.having import HavingClauseGenerator
from sqlbuster.clauses.limit import LimitClauseGenerator
from sqlbuster.clauses.order_by import OrderByClauseGenerator
from sqlbuster.clauses.select import SelectClauseGenerator
from sqlbuster.clauses.where import WhereClauseGenerator
from sqlbuster.core.engine import ClauseType, EvolvingEngine
from sqlbuster.core.registry import ColumnSchema, SchemaRegistry
from sqlbuster.core.runner import MockRunner
from sqlbuster.reporter.reporter import Reporter


def setup_registry() -> SchemaRegistry:
    """
    Setup schema registry

    Create a table with multiple columns of the same type to demonstrate representative sampling effect.

    Returns:
        Configured SchemaRegistry instance
    """
    registry = SchemaRegistry()

    # Register a test table with multiple columns of the same type
    # For demonstrating representative sampling: when int type col1 succeeds in WHERE clause,
    # col2 and col3 will no longer generate WHERE variants
    columns = [
        ColumnSchema(name="id", type=int, is_nullable=False),
        ColumnSchema(name="name", type=str),
        ColumnSchema(name="age", type=int),  # int类型，同类型列1
        ColumnSchema(name="score", type=int),  # int类型，同类型列2
        ColumnSchema(name="count", type=int),  # int类型，同类型列3
        ColumnSchema(name="description", type=str),  # str类型，同类型列1
        ColumnSchema(name="address", type=str),  # str类型，同类型列2
        ColumnSchema(name="grade", type=float),
    ]
    registry.register_table("students", columns)

    return registry


def setup_runner() -> MockRunner:
    """
    Setup mock runner

    Simulates a database that does not support HAVING clause.

    Returns:
        Configured MockRunner instance
    """
    # Does not support HAVING clause
    runner = MockRunner({"HAVING"})
    return runner


def setup_engine(registry: SchemaRegistry, runner: MockRunner) -> EvolvingEngine:
    """
    Setup evolution engine

    Use the new register_clause_generator function to register clause generators,
    supports representative sampling optimization.

    Args:
        registry: Schema registry
        runner: SQL executor

    Returns:
        Configured EvolvingEngine instance
    """
    engine = EvolvingEngine(registry=registry, runner=runner, start_table="students")

    # Use the new register_clause_generator function to register clause generators
    # This function automatically handles the passing of representative sampling cache
    register_clause_generator(
        engine, ClauseType.SELECT, SelectClauseGenerator, registry
    )
    register_clause_generator(engine, ClauseType.WHERE, WhereClauseGenerator, registry)
    register_clause_generator(
        engine, ClauseType.GROUP_BY, GroupByClauseGenerator, registry
    )
    register_clause_generator(
        engine, ClauseType.HAVING, HavingClauseGenerator, registry
    )
    register_clause_generator(
        engine, ClauseType.ORDER_BY, OrderByClauseGenerator, registry
    )
    register_clause_generator(engine, ClauseType.LIMIT, LimitClauseGenerator, registry)

    return engine


def main() -> None:
    """Main function, runs SQL capability boundary exploration example"""
    # Configure logging to view representative sampling information
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )

    print("=" * 60)
    print("SQLBuster - SQL Capability Boundary Exploration Example")
    print("模拟一个Does not support HAVING clause的数据库")
    print("Demonstrate representative sampling optimization effect")
    print("=" * 60)
    print()

    # 1. Setup components
    print("[1] Setup schema registry...")
    registry = setup_registry()
    print(f"    Registered tables: {registry.list_tables()}")
    table = registry.get_table("students")
    if table is not None:
        print(f"    Table columns: {[col.name for col in table.columns]}")
        print(f"    Column type distribution:")
        type_count: Dict[str, int] = {}
        for col in table.columns:
            type_name = col.type.__name__
            type_count[type_name] = type_count.get(type_name, 0) + 1
        for type_name, count in type_count.items():
            print(f"      {type_name}: {count} 列")
    print()

    print("[2] Setup mock runner...")
    runner = setup_runner()
    print(f"    Unsupported clauses: {runner.list_unsupported_clauses()}")
    print()

    print("[3] Setup evolution engine（启用代表性采样优化）...")
    engine = setup_engine(registry, runner)
    print()

    # 2. Create reporter
    reporter = Reporter()

    # 3. Start exploration
    print("[4] Start explorationSQL能力边界...")
    print("-" * 60)

    success_count = 0
    fail_count = 0
    representative_count = 0

    for node in engine.explore():
        # Record to report
        reporter.add_execution_result(node)

        # Display results
        status = "✓" if node.success else "✗"
        clause_info = f"[{node.clause_type.value}]" if node.clause_type else "[ROOT]"

        # Check if it is representative sampling
        is_representative = False
        if hasattr(node, "clause_type") and node.clause_type:
            # Infer from SQL (simplified here, should actually get from variant's metadata)
            pass

        print(f"{status} {clause_info} {node.sql[:70]}")

        if node.success:
            success_count += 1
        else:
            fail_count += 1
            if node.error_msg:
                print(f"    错误: {node.error_msg}")

    print("-" * 60)
    print()

    # 4. 显示统计信息
    print("[5] Exploration completed, statistics:")
    print("-" * 60)
    stats = reporter.get_statistics()
    print(f"Total executions: {stats['total_executions']}")
    print(f"Successful executions: {stats['successful_executions']}")
    print(f"Failed executions: {stats['failed_executions']}")
    print(f"Success rate: {stats['success_rate']:.2%}")
    print()

    # 按Clause type显示统计
    print("Statistics by clause type:")
    for clause_type, stat in stats["clause_type_statistics"].items():
        rate = stat["success"] / stat["total"] if stat["total"] > 0 else 0
        print(f"  {clause_type}: {stat['success']}/{stat['total']} ({rate:.2%})")
    print()

    # 5. 显示类型成功缓存（代表性采样效果）
    print("[6] Representative sampling cache status:")
    print("-" * 60)
    type_cache = engine.get_type_success_cache()
    if type_cache:
        for (table_name, clause_type, col_type), success in type_cache.items():
            if success:
                print(
                    f"  表={table_name}, 子句={clause_type.value}, 类型={col_type.__name__}: 已成功"
                )
        print(f"\n共有 {len(type_cache)} 个类型-子句组合被缓存")
    else:
        print("  Cache is empty")
    print()

    # 6. 保存报告
    print("[7] Saving report...")
    report_path = "sqlbuster_report.json"
    reporter.save_report(report_path, format="json")
    print(f"    JSON report saved to: {report_path}")

    # Can also save text tree report
    tree_path = "sqlbuster_report.txt"
    reporter.save_report(tree_path, format="tree")
    print(f"    Text report saved to: {tree_path}")
    print()

    print("=" * 60)
    print("Example run completed!")
    print("=" * 60)
    print()
    print("Representative sampling optimization description:")
    print("- When a column of a type succeeds in testing a clause,")
    print(
        "  other columns of this type on this clause will only generate representative variants"
    )
    print("- This greatly reduces redundant tests and improves exploration efficiency")
    print("- Check the log output above for 'Using representative sampling' messages")


if __name__ == "__main__":
    main()
