"""冻结裁决存储。

语义：
- 审计标识首次提交：计算裁决并永久冻结（进程生命周期内不可变）。
- 同标识 + 内容完全相同（规范哈希一致）：返回同一份冻结结论（幂等）。
- 同标识 + 内容不同：409 冲突，原裁决保持不变。
- 读取接口永远只返回已冻结的结论。
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from typing import Any

from .models import ScheduleRequest
from .scheduler import (
    InstanceNotFound,
    Scheduler,
    Untraceable,
    canonical_hash,
)


class ConflictError(Exception):
    def __init__(self, audit_id: str, stored_hash: str, incoming_hash: str):
        self.audit_id = audit_id
        self.stored_hash = stored_hash
        self.incoming_hash = incoming_hash
        super().__init__(f"审计标识 {audit_id} 已冻结不同内容的裁决")


@dataclass(frozen=True)
class FrozenDecision:
    audit_id: str
    content_hash: str
    frozen_at: float
    request: dict[str, Any]
    verdict: dict[str, Any]
    # 运行结束后只读的模拟引擎，供等待链实例查询按冻结时的连续时隙重建证据；
    # 不进入 to_json，也不会被查询修改。
    engine: Any = None

    def to_json(self) -> dict[str, Any]:
        return {
            "audit_id": self.audit_id,
            "content_hash": self.content_hash,
            "frozen_at": self.frozen_at,
            "frozen": True,
            "request": self.request,
            "decision": self.verdict,
        }


class DecisionStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._decisions: dict[str, FrozenDecision] = {}

    def submit(self, req: ScheduleRequest) -> tuple[FrozenDecision, bool]:
        """提交并冻结。返回 (裁决, 是否本次新建)。冲突时抛 ConflictError。"""
        content_hash = canonical_hash(req)
        with self._lock:
            existing = self._decisions.get(req.audit_id)
            if existing is not None:
                if existing.content_hash != content_hash:
                    raise ConflictError(
                        req.audit_id, existing.content_hash, content_hash
                    )
                return existing, False
            engine = Scheduler(req)
            verdict = engine.run()
            decision = FrozenDecision(
                audit_id=req.audit_id,
                content_hash=content_hash,
                frozen_at=time.time(),
                request=req.to_json(),
                verdict=verdict,
                engine=engine,
            )
            self._decisions[req.audit_id] = decision
            return decision, True

    def get(self, audit_id: str) -> FrozenDecision | None:
        with self._lock:
            return self._decisions.get(audit_id)

    def wait_chain(self, audit_id: str, frame: str) -> dict[str, Any]:
        """对已冻结裁决重建指定实例的连续等待链。

        - 裁决不存在 / 实例从未出现 -> InstanceNotFound；
        - 证据无法连续覆盖 -> Untraceable。
        只读访问，不改变任何冻结结果。
        """
        with self._lock:
            decision = self._decisions.get(audit_id)
        if decision is None or decision.engine is None:
            raise InstanceNotFound(
                f"审计标识 {audit_id} 无可追溯的冻结裁决"
            )
        return decision.engine.wait_chain(frame)

    def list_ids(self) -> list[str]:
        with self._lock:
            return sorted(self._decisions)
