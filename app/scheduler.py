"""事件驱动的 TAS 门控调度模拟与裁决引擎。

语义约定（整数微秒）：
- 严格优先级：priority 数值越小优先级越高；同优先级内按流、按序号 FIFO。
- 非抢占：帧一旦开始发送即占满整个发送时长，门控中途关闭也继续发完。
- 门控仅放行列明优先级；候选帧若在当前开放窗口剩余时间内无法完整发送，
  则继续等待（不开始发送）；更低优先级若能在其窗口内完整发送则可使用链路。
- 流实例在 k*period 释放并立即入队（入队时间==释放时间）。同一流的新实例
  追加在未发送帧之后（FIFO），绝不覆盖未发送帧。
- 完成时刻 <= release+deadline 视为按期；在截止时刻仍未完成即为超期。

裁决优先级：
1. 连续两个超周期（hyperperiod）边界遗留队列严格增长 -> NON_CONVERGENT，
   以队列增长证据拒绝（释放与门控在超周期内稳态重复，增长必然无界）；
2. 已出现超期帧，则继续模拟到其后第一个超周期边界：边界遗留未增长
   （系统能追平队列）-> DEADLINE_MISS，给出首个超期帧的释放/入队/
   起止发送时间与阻塞来源；
3. 连续两个超周期边界排空且链路空闲 -> SCHEDULABLE，给出全部流按期证据。
任何达到模拟上限仍不能收敛的情况一律拒绝，绝不截断模拟后判定通过。
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from hashlib import sha256
from math import gcd
from typing import Any

from .models import ScheduleRequest

MAX_GATE_CYCLES = 4000          # 模拟门控周期数硬上限，防止无界运行
MAX_SEGMENTS = 20000            # 逐时隙记录上限，控制冻结结论载荷
GROWTH_CONFIRMATIONS = 2        # 超周期边界连续增长两次即确认无法收敛
DRAIN_PROOFS_NEEDED = 2         # 连续两个超周期边界排空才判定可调度


def lcm(a: int, b: int) -> int:
    return a // gcd(a, b) * b


@dataclass
class Frame:
    flow_id: str
    priority: int
    seq: int
    release: int
    transmit_time: int
    deadline_abs: int
    status: str = "queued"       # queued | sending | sent | late
    start: int | None = None
    finish: int | None = None

    @property
    def fid(self) -> str:
        return f"{self.flow_id}#{self.seq}"


@dataclass
class Segment:
    """一段状态不变的时隙 [start, end)。"""

    start: int
    end: int
    gate_open: list[int]
    queues: dict[str, list[str]]
    transmitting: dict[str, Any] | None


@dataclass
class _Bound:
    t: int
    pending: int
    before_release: bool = True


class Scheduler:
    def __init__(self, req: ScheduleRequest):
        self.req = req
        self.G = req.gate_period
        # 门控状态只可能在这些周期相对时刻发生变化。
        changes = {0, self.G}
        for ge in req.gate_entries:
            changes.add(ge.start)
            changes.add(ge.end)
        self.gate_changes = sorted(changes)

        h = self.G
        for f in req.flows:
            h = lcm(h, f.period)
        self.H = h

        self.queues: dict[int, list[Frame]] = {f.priority: [] for f in req.flows}
        self.next_seq: dict[str, int] = {f.flow_id: 0 for f in req.flows}
        self.in_flight: Frame | None = None

        self.segments: list[Segment] = []
        self.events: list[dict[str, Any]] = []
        self.transmissions: list[dict[str, Any]] = []
        self.bound_samples: list[_Bound] = []
        self.late_frames: list[Frame] = []
        self.first_late: Frame | None = None

        self.growth_chain: list[_Bound] = []  # 超周期边界连续增长链
        self._prev_h_pending: int | None = None
        self._growth_streak = 0
        self._drain_proofs = 0
        # 最坏情况下首个超期帧接近 2H，需要再观察一个超周期，故覆盖到 3H。
        self.horizon = min(self.G * MAX_GATE_CYCLES, self.H * 3 + self.G)
        self._seg_start = 0

    # ---- 门控查询 ----

    def gate_open(self, t: int) -> frozenset[int]:
        return self.req.gate_mask(t % self.G)

    def _window_end_at(self, t: int, priority: int) -> int:
        """t 时刻 priority 开放，返回当前所在门控项（周期化）的结束时刻。"""
        r = t % self.G
        for ge in self.req.gate_entries:
            if ge.start <= r < ge.end and priority in ge.priorities:
                return (t - r) + ge.end
        return t  # 理论不可达（调用方已确认开放）

    def next_gate_change(self, t: int) -> int:
        r = t % self.G
        for c in self.gate_changes:
            if c > r:
                return t + (c - r)
        return t + (self.G - r)

    # ---- 帧管理 ----

    def _active_frames(self) -> list[Frame]:
        out: list[Frame] = []
        for p in sorted(self.queues):
            out.extend(fr for fr in self.queues[p] if fr.status in ("queued", "late"))
        if self.in_flight is not None:
            out.append(self.in_flight)
        return out

    def _pending_total(self) -> int:
        return sum(
            sum(1 for fr in q if fr.status in ("queued", "late"))
            for q in self.queues.values()
        ) + (1 if self.in_flight is not None else 0)

    def _next_release(self, t: int) -> int | None:
        nxt: int | None = None
        for f in self.req.flows:
            rel = self.next_seq[f.flow_id] * f.period
            if rel >= t and (nxt is None or rel < nxt):
                nxt = rel
        return nxt

    def _next_deadline(self, t: int) -> int | None:
        nxt: int | None = None
        for fr in self._active_frames():
            if fr.status in ("queued", "sending") and fr.deadline_abs >= t:
                if nxt is None or fr.deadline_abs < nxt:
                    nxt = fr.deadline_abs
        return nxt

    # ---- 时隙记录 ----

    def _close_segment(self, end: int, gate_at_start: frozenset[int]) -> None:
        if end <= self._seg_start:
            return
        queues_snap = {
            str(p): [fr.fid for fr in self.queues[p]] for p in sorted(self.queues)
        }
        tx = None
        if self.in_flight is not None:
            tx = {
                "frame": self.in_flight.fid,
                "flow_id": self.in_flight.flow_id,
                "priority": self.in_flight.priority,
                "start": self.in_flight.start,
                "finish": self.in_flight.finish,
                "deadline": self.in_flight.deadline_abs,
                "on_time": self.in_flight.finish <= self.in_flight.deadline_abs,
            }
        self.segments.append(
            Segment(
                start=self._seg_start,
                end=end,
                gate_open=sorted(gate_at_start),
                queues=queues_snap,
                transmitting=tx,
            )
        )
        self._seg_start = end

    # ---- 主模拟 ----

    def run(self) -> dict[str, Any]:
        t = 0
        gate_now = self.gate_open(t)

        # t=0：先采样空遗留队列，再释放首批实例并尝试启动。
        self._sample_boundary(t, before_release=True)
        self._release_at(t)
        self._try_start(t, gate_now)

        verdict: dict[str, Any] | None = None
        while verdict is None:
            candidates = [
                self.in_flight.finish if self.in_flight is not None else None,
                self._next_release(t + 1),
                self.next_gate_change(t),
                self._next_deadline(t + 1),
            ]
            candidates = [c for c in candidates if c is not None]
            if not candidates:
                break
            nt = min(candidates)
            if nt > self.horizon:
                verdict = self._verdict_non_convergent(
                    reason="达到模拟上限仍未在周期边界收敛，拒绝判定通过"
                )
                break

            self._close_segment(nt, gate_now)
            t = nt
            gate_now = self.gate_open(t)

            # 1) 恰在时刻 t 发完的帧先完成（[start,finish) 左闭右开，
            #    边界时刻发完的帧不计入跨周期遗留）。
            if self.in_flight is not None and self.in_flight.finish == t:
                self._complete(t)
            # 2) 新释放之前采样门控周期边界，度量真实“遗留队列”。
            if t % self.G == 0:
                self._sample_boundary(t, before_release=True)
            # 3) 新实例追加进队（绝不覆盖未发送帧）。
            self._release_at(t)
            # 4) 截止期检查（含正在非抢占发送中的帧）。
            self._mark_late(t)
            # 5) 空闲链路尝试按严格优先级启动。
            if self.in_flight is None:
                self._try_start(t, gate_now)

            verdict = self._verdict_after_event(t)

        if verdict is None:
            verdict = self._verdict_non_convergent(reason="模拟结束时状态未收敛")
        return verdict

    def _release_at(self, t: int) -> None:
        for f in self.req.flows:
            seq = self.next_seq[f.flow_id]
            if seq * f.period == t:
                frame = Frame(
                    flow_id=f.flow_id,
                    priority=f.priority,
                    seq=seq,
                    release=t,
                    transmit_time=f.transmit_time,
                    deadline_abs=t + f.deadline,
                )
                # 追加在同流未发送帧之后，不覆盖任何旧帧。
                self.queues[f.priority].append(frame)
                self.next_seq[f.flow_id] = seq + 1
                self.events.append(
                    {
                        "t": t,
                        "type": "release",
                        "frame": frame.fid,
                        "flow_id": f.flow_id,
                        "priority": f.priority,
                        "enqueue": t,
                        "deadline": frame.deadline_abs,
                    }
                )

    def _complete(self, t: int) -> None:
        frame = self.in_flight
        assert frame is not None
        on_time = t <= frame.deadline_abs
        self.in_flight = None
        if not on_time:
            # 非抢占：发送越过截止期，帧已超期但仍占满发送时长。
            # 若该帧此前已在队中被记为超期，不重复记账。
            if frame not in self.late_frames:
                frame.status = "late"
                self.late_frames.append(frame)
                if self.first_late is None:
                    self.first_late = frame
        else:
            frame.status = "sent"
        frame.finish = t
        self.transmissions.append(
            {
                "frame": frame.fid,
                "flow_id": frame.flow_id,
                "priority": frame.priority,
                "seq": frame.seq,
                "release": frame.release,
                "enqueue": frame.release,
                "start": frame.start,
                "finish": t,
                "deadline": frame.deadline_abs,
                "on_time": on_time,
            }
        )
        self.events.append(
            {
                "t": t,
                "type": "complete",
                "frame": frame.fid,
                "on_time": on_time,
            }
        )

    def _sample_boundary(self, t: int, before_release: bool) -> None:
        if t % self.G != 0:
            return
        pending = self._pending_total()
        self.bound_samples.append(
            _Bound(t, pending, before_release=before_release)
        )

        # 增长判据只在超周期边界上比较：释放与门控在每个超周期内稳态
        # 重复，该边界积压量关于时间单调，故“未出现下降的两次严格增长”
        # （允许中间持平）即证明积压无界。
        if t % self.H == 0:
            if t > 0 and self._prev_h_pending is not None:
                if pending > self._prev_h_pending:
                    self._growth_streak += 1
                    self.growth_chain.append(_Bound(t, pending, before_release=True))
                elif pending < self._prev_h_pending:
                    # 只有真正回落才中断增长链；持平不中断。
                    self._growth_streak = 0
                    self.growth_chain = []
            self._prev_h_pending = pending
            if t > 0 and pending == 0:
                self._drain_proofs += 1

    def _mark_late(self, t: int) -> None:
        for frame in self._active_frames():
            if frame.status in ("queued", "sending") and frame.deadline_abs <= t:
                frame.status = "late"
                self.late_frames.append(frame)
                if self.first_late is None:
                    self.first_late = frame
                self.events.append(
                    {"t": t, "type": "deadline_miss", "frame": frame.fid}
                )

    def _verdict_after_event(self, t: int) -> dict[str, Any] | None:
        # 1) 连续两个超周期边界遗留增长坐实：无法收敛，必须拒绝
        #    （即便此时已有帧超期，增长证据优先）。
        if self._growth_streak >= GROWTH_CONFIRMATIONS:
            return self._verdict_non_convergent(
                reason="队列在连续两个超周期边界严格增长，周期边界无法收敛"
            )
        # 2) 首个超期帧出现后继续观察到其后（不含超期时刻所在边界）的
        #    超周期边界：
        #    - 该边界处增长链已复位（遗留未再增长）-> DEADLINE_MISS；
        #    - 增长链计数为 1 -> 尚不能排除继续无界增长，再观察一个超周期
        #      （届时由规则 1 拒绝或由本规则判超期）。
        if self.first_late is not None and t % self.H == 0:
            miss_deadline = self.first_late.deadline_abs
            if t > miss_deadline and self._growth_streak == 0:
                return self._verdict_miss(self.first_late, t)
        # 3) 连续两个超周期边界排空且链路空闲：可调度。
        if self.first_late is None and self._drain_proofs >= DRAIN_PROOFS_NEEDED:
            return self._verdict_schedulable()
        return None

    def _try_start(self, t: int, gate_now: frozenset[int]) -> None:
        # 数值越小优先级越高；高优先级放不下时不阻塞低优先级使用本窗口。
        for p in sorted(self.queues):
            q = self.queues[p]
            # 已超期帧保留在 FIFO 队首并继续被服务（其超期已记录为证据，
            # 服务后队列回落，避免人为制造积压）。
            head = 0
            while head < len(q) and q[head].status not in ("queued", "late"):
                head += 1
            if head >= len(q) or p not in gate_now:
                continue
            frame = q[head]
            # 门控项关门前不能完整发送 -> 不启动，帧继续等待。
            if self._window_end_at(t, p) - t < frame.transmit_time:
                continue
            frame.status = "sending"
            frame.start = t
            frame.finish = t + frame.transmit_time
            self.in_flight = frame
            q.pop(head)
            self.events.append(
                {
                    "t": t,
                    "type": "start",
                    "frame": frame.fid,
                    "flow_id": frame.flow_id,
                    "priority": p,
                    "finish": frame.finish,
                    "window_end": self._window_end_at(t, p),
                }
            )
            return

    # ---- 阻塞来源分析 ----

    def _blockers_for(self, frame: Frame, until: int) -> list[dict[str, Any]]:
        """分析帧在 [release, until) 内未能按时发送的阻塞来源。"""
        blockers: list[dict[str, Any]] = []
        start_limit = frame.start if frame.start is not None else until
        for seg in self.segments:
            s = max(seg.start, frame.release)
            e = min(seg.end, start_limit)
            if s >= e:
                continue
            tx = seg.transmitting
            if tx is not None and tx["frame"] != frame.fid and tx["priority"] < frame.priority:
                blockers.append(
                    {
                        "type": "higher_priority_tx",
                        "from": s,
                        "to": e,
                        "source_frame": tx["frame"],
                        "source_flow": tx["flow_id"],
                        "source_priority": tx["priority"],
                        "detail": "高优先级帧非抢占占用单出口",
                    }
                )
            elif tx is not None and tx["frame"] != frame.fid:
                blockers.append(
                    {
                        "type": "nonpreemptive_hold",
                        "from": s,
                        "to": e,
                        "source_frame": tx["frame"],
                        "source_flow": tx["flow_id"],
                        "source_priority": tx["priority"],
                        "detail": "帧发送中，非抢占规则下跨周期遗留占用链路",
                    }
                )
            elif frame.priority not in seg.gate_open:
                blockers.append(
                    {
                        "type": "gate_closed",
                        "from": s,
                        "to": e,
                        "detail": f"优先级 {frame.priority} 的门控项未开放",
                    }
                )
            else:
                close = self._window_end_at(s, frame.priority)
                remain = close - s
                if remain < frame.transmit_time:
                    blockers.append(
                        {
                            "type": "window_too_short",
                            "from": s,
                            "to": e,
                            "detail": (
                                f"门窗口剩余 {remain}us 小于发送时长 "
                                f"{frame.transmit_time}us，无法完整发送，继续等待"
                            ),
                        }
                    )
        # 合并相邻同类且同源记录，便于页面阅读。
        merged: list[dict[str, Any]] = []
        for b in blockers:
            if (
                merged
                and merged[-1]["type"] == b["type"]
                and merged[-1]["to"] == b["from"]
                and merged[-1].get("source_frame") == b.get("source_frame")
            ):
                merged[-1]["to"] = b["to"]
                continue
            merged.append(dict(b))
        return merged

    # ---- 裁决组装 ----

    def _verdict_schedulable(self) -> dict[str, Any]:
        proof_end = self.H * DRAIN_PROOFS_NEEDED
        per_flow: dict[str, list[dict[str, Any]]] = {
            f.flow_id: [] for f in self.req.flows
        }
        for tr in self.transmissions:
            if tr["finish"] <= proof_end and tr["on_time"]:
                per_flow[tr["flow_id"]].append(tr)
        return {
            "verdict": "SCHEDULABLE",
            "summary": "连续两个超周期边界队列排空且链路空闲，全部流按期发送",
            "hyperperiod": self.H,
            "proof_window": [0, proof_end],
            "flow_evidence": per_flow,
            "cycle_snapshots": [
                {"t": b.t, "pending": b.pending, "drained": b.pending == 0}
                for b in self.bound_samples
                if b.t <= proof_end
            ],
            "events": [e for e in self.events if e["t"] <= proof_end],
            "timeline": self._timeline(proof_end),
        }

    def _verdict_miss(self, frame: Frame, at_t: int) -> dict[str, Any]:
        blockers = self._blockers_for(frame, at_t)
        started_then_late = frame.start is not None and frame.status == "late"
        return {
            "verdict": "DEADLINE_MISS",
            "summary": f"首个超期帧 {frame.fid} 在 {frame.deadline_abs}us 超出截止期",
            "first_overdue_frame": {
                "frame": frame.fid,
                "flow_id": frame.flow_id,
                "priority": frame.priority,
                "seq": frame.seq,
                "release": frame.release,
                "enqueue": frame.release,
                "transmit_start": frame.start,
                "transmit_end": frame.finish if started_then_late else None,
                "deadline": frame.deadline_abs,
                "detected_at": frame.deadline_abs,
                "state_when_overdue": (
                    "transmitting" if frame.start is not None else "waiting"
                ),
                "note": (
                    "帧已开始发送但非抢占发送越过截止期"
                    if started_then_late
                    else "帧在截止时刻仍未获得完整发送窗口"
                ),
                "blockers": blockers,
            },
            "events": [e for e in self.events if e["t"] <= at_t],
            "timeline": self._timeline(at_t),
            "boundary_samples": [
                {"t": b.t, "pending": b.pending} for b in self.bound_samples if b.t <= at_t
            ],
        }

    def _verdict_non_convergent(self, reason: str) -> dict[str, Any]:
        end_t = self.bound_samples[-1].t if self.bound_samples else 0
        return {
            "verdict": "NON_CONVERGENT",
            "summary": reason,
            "queue_growth": [
                {"t": b.t, "pending": b.pending} for b in self.bound_samples
            ],
            "growth_chain": [
                {"t": b.t, "pending": b.pending} for b in self.growth_chain
            ],
            "late_frames": [
                {
                    "frame": fr.fid,
                    "flow_id": fr.flow_id,
                    "priority": fr.priority,
                    "release": fr.release,
                    "deadline": fr.deadline_abs,
                    "transmit_start": fr.start,
                }
                for fr in self.late_frames
            ],
            "events": self.events,
            "timeline": self._timeline(end_t),
        }

    def _timeline(self, end: int) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        truncated = False
        for seg in self.segments:
            if seg.start >= end:
                break
            if len(out) >= MAX_SEGMENTS:
                truncated = True
                break
            out.append(
                {
                    "start": seg.start,
                    "end": min(seg.end, end),
                    "gate_open": seg.gate_open,
                    "queues": seg.queues,
                    "transmitting": seg.transmitting,
                }
            )
        if truncated:
            out.append(
                {
                    "start": out[-1]["end"] if out else 0,
                    "end": end,
                    "gate_open": [],
                    "queues": {},
                    "transmitting": None,
                    "note": f"时间线在 {MAX_SEGMENTS} 个时隙后截断（仅展示，不影响裁决）",
                }
            )
        return out


def adjudicate(req: ScheduleRequest) -> dict[str, Any]:
    return Scheduler(req).run()


def canonical_hash(req: ScheduleRequest) -> str:
    """对提交内容做规范序列化，用于冻结/冲突判定。"""
    blob = json.dumps(req.to_json(), sort_keys=True, ensure_ascii=False)
    return sha256(blob.encode("utf-8")).hexdigest()
