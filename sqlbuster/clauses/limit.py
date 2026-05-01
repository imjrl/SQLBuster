"""
LIMIT子句生成器模块

本模块实现了LIMIT子句的变体生成器，支持生成不同形式的LIMIT子句，
包括简单LIMIT和LIMIT OFFSET形式。
支持代表性采样优化（虽然LIMIT不涉及列类型，但仍保持接口一致）。
"""

from typing import Any, Dict, List, Optional

from sqlbuster.clauses.base import BaseClauseGenerator
from sqlbuster.core.engine import ClauseType, ClauseVariant
from sqlbuster.core.registry import SchemaRegistry


class LimitClauseGenerator(BaseClauseGenerator):
    """
    LIMIT子句生成器

    生成LIMIT子句的各种变体，包括：
    - 简单LIMIT (LIMIT 10)
    - LIMIT OFFSET (LIMIT 10 OFFSET 20)
    - LIMIT row_count OFFSET offset 形式

    支持代表性采样优化（虽然LIMIT不涉及列类型，但仍保持接口一致）。
    """

    def generate(
        self,
        clause_type: ClauseType,
        registry: Optional[SchemaRegistry] = None,
        type_success_cache: Optional[Dict] = None,
    ) -> List[ClauseVariant]:
        """
        生成LIMIT子句变体

        Args:
            clause_type: Clause type（应为ClauseType.LIMIT）
            registry: Schema registry (optional, priority given to initialized registry)
            type_success_cache: 类型成功缓存（LIMIT子句不使用，保持接口一致）

        Returns:
            LIMIT子句变体列表
        """
        if clause_type != ClauseType.LIMIT:
            return []

        variants = []

        # 简单LIMIT
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.LIMIT,
                sql_fragment="LIMIT 10",
                priority=0,
                metadata={"type": "limit"},
            )
        )

        # LIMIT 1（返回单条记录）
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.LIMIT,
                sql_fragment="LIMIT 1",
                priority=1,
                metadata={"type": "limit"},
            )
        )

        # LIMIT OFFSET 形式
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.LIMIT,
                sql_fragment="LIMIT 10 OFFSET 20",
                priority=2,
                metadata={"type": "limit_offset"},
            )
        )

        # 较大的LIMIT
        variants.append(
            ClauseVariant(
                clause_type=ClauseType.LIMIT,
                sql_fragment="LIMIT 100",
                priority=3,
                metadata={"type": "limit"},
            )
        )

        return variants
