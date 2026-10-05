"""请求模型与输入校验。

所有时间单位为整数微秒（us）。校验规则：
- 审计标识为非空稳定字符串；
- 门控周期为正整数微秒；
- 流 1..8 条，每条含唯一优先级、正周期、正发送时长、相对截止期；
- 门控项按时间升序，start/end 落在周期内，同优先级窗口互不重叠，
  每个被门控引用的优先级都必须存在对应流。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class ValidationError(ValueError):
    """输入数据不合法。"""


@dataclass(frozen=True)
class Flow:
    flow_id: str
    priority: int
    period: int          # 释放周期（us）
    transmit_time: int   # 单帧发送时长（us）
    deadline: int        # 相对释放点的截止期（us）


@dataclass(frozen=True)
class GateEntry:
    start: int
    end: int
    priorities: tuple[int, ...]


@dataclass
class ScheduleRequest:
    audit_id: str
    gate_period: int
    flows: list[Flow] = field(default_factory=list)
    gate_entries: list[GateEntry] = field(default_factory=list)

    def gate_mask(self, t: int) -> frozenset[int]:
        """返回时刻 t（相对周期起点）门控开放的优先级集合。"""
        open_prios: set[int] = set()
        for ge in self.gate_entries:
            if ge.start <= t < ge.end:
                open_prios.update(ge.priorities)
        return frozenset(open_prios)

    def to_json(self) -> dict[str, Any]:
        return {
            "audit_id": self.audit_id,
            "gate_period": self.gate_period,
            "flows": [
                {
                    "flow_id": f.flow_id,
                    "priority": f.priority,
                    "period": f.period,
                    "transmit_time": f.transmit_time,
                    "deadline": f.deadline,
                }
                for f in self.flows
            ],
            "gate_entries": [
                {
                    "start": ge.start,
                    "end": ge.end,
                    "priorities": list(ge.priorities),
                }
                for ge in self.gate_entries
            ],
        }


def _as_int(value: Any, field_name: str) -> int:
    # bool 是 int 的子类，显式拒绝以免 True 被当作 1。
    if isinstance(value, bool) or not isinstance(value, int):
        raise ValidationError(f"{field_name} 必须是整数微秒")
    return value


def parse_request(data: Any) -> ScheduleRequest:
    if not isinstance(data, dict):
        raise ValidationError("请求体必须是 JSON 对象")

    raw_audit = data.get("audit_id")
    if not isinstance(raw_audit, str) or not raw_audit.strip():
        raise ValidationError("audit_id 必须是非空稳定审计标识")
    audit_id = raw_audit.strip()

    gate_period = _as_int(data.get("gate_period"), "gate_period")
    if gate_period <= 0:
        raise ValidationError("gate_period 必须是正整数")

    raw_flows = data.get("flows")
    if not isinstance(raw_flows, list) or not (1 <= len(raw_flows) <= 8):
        raise ValidationError("flows 必须包含 1 至 8 条流")

    flows: list[Flow] = []
    seen_priorities: set[int] = set()
    seen_ids: set[str] = set()
    for i, item in enumerate(raw_flows):
        if not isinstance(item, dict):
            raise ValidationError(f"flows[{i}] 必须是对象")
        flow_id = item.get("flow_id", f"F{i}")
        if not isinstance(flow_id, str) or not flow_id.strip():
            raise ValidationError(f"flows[{i}].flow_id 必须是非空字符串")
        flow_id = flow_id.strip()
        if flow_id in seen_ids:
            raise ValidationError(f"flow_id 重复: {flow_id}")
        seen_ids.add(flow_id)

        priority = _as_int(item.get("priority"), f"flows[{i}].priority")
        if not (0 <= priority <= 7):
            raise ValidationError(f"flows[{i}].priority 必须在 0..7 之间")
        if priority in seen_priorities:
            raise ValidationError(f"优先级重复: {priority}")
        seen_priorities.add(priority)

        period = _as_int(item.get("period"), f"flows[{i}].period")
        transmit_time = _as_int(
            item.get("transmit_time"), f"flows[{i}].transmit_time"
        )
        deadline = _as_int(item.get("deadline"), f"flows[{i}].deadline")
        if period <= 0 or transmit_time <= 0 or deadline <= 0:
            raise ValidationError(f"flows[{i}] 的周期/发送时长/截止期必须为正")
        if transmit_time > period:
            raise ValidationError(
                f"flows[{i}] 发送时长 {transmit_time} 超过自身周期 {period}"
            )
        if deadline < transmit_time:
            raise ValidationError(
                f"flows[{i}] 截止期 {deadline} 短于发送时长 {transmit_time}，"
                "物理上无法按期完成"
            )
        if deadline > period:
            raise ValidationError(
                f"flows[{i}] 截止期 {deadline} 不能晚于周期 {period}"
            )
        flows.append(Flow(flow_id, priority, period, transmit_time, deadline))

    raw_entries = data.get("gate_entries")
    if not isinstance(raw_entries, list) or not raw_entries:
        raise ValidationError("gate_entries 至少包含一个门控项")

    gate_entries: list[GateEntry] = []
    prev_end = 0
    # 同优先级窗口重叠检测：按优先级记录已占用的 [start,end)。
    prio_windows: dict[int, list[tuple[int, int]]] = {}
    referenced: set[int] = set()
    for i, item in enumerate(raw_entries):
        if not isinstance(item, dict):
            raise ValidationError(f"gate_entries[{i}] 必须是对象")
        start = _as_int(item.get("start"), f"gate_entries[{i}].start")
        end = _as_int(item.get("end"), f"gate_entries[{i}].end")
        if not (0 <= start < end <= gate_period):
            raise ValidationError(
                f"gate_entries[{i}] 须满足 0 <= start < end <= gate_period"
            )
        if start < prev_end:
            raise ValidationError(f"gate_entries[{i}] 必须按时间升序排列")
        prev_end = end

        prios = item.get("priorities")
        if not isinstance(prios, list) or not prios:
            raise ValidationError(f"gate_entries[{i}] 必须列明至少一个优先级")
        prio_tuple: list[int] = []
        for p in prios:
            p = _as_int(p, f"gate_entries[{i}].priorities")
            if p in prio_tuple:
                raise ValidationError(f"gate_entries[{i}] 中优先级 {p} 重复")
            prio_tuple.append(p)
            referenced.add(p)
            for ws, we in prio_windows.setdefault(p, []):
                if start < we and ws < end:
                    raise ValidationError(
                        f"优先级 {p} 的门控窗口重叠: "
                        f"[{ws},{we}) 与 [{start},{end})"
                    )
            prio_windows[p].append((start, end))
        gate_entries.append(GateEntry(start, end, tuple(prio_tuple)))

    unknown = referenced - seen_priorities
    if unknown:
        raise ValidationError(
            f"门控项引用了不存在的流优先级: {sorted(unknown)}"
        )

    return ScheduleRequest(audit_id, gate_period, flows, gate_entries)
