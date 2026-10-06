"""事件驱动的 TAS 门控调度模拟与裁决引擎。

语义约定（整数微秒）：
- 严格优先级：priority 数值越小优先级越高；同优先级内按流、按序号 FIFO。
- 非抢占：帧一旦开始发送即占满整个发送时长，门控中途关闭也继续发完。
- 门控仅放行列明优先级；候选帧若在当前开放窗口剩余时间内无法完整发送，
  则继续等待（不开始发送）；更低优先级若能在其窗口内完整发送则可使用链路。
- 流实例在 k*period 释放并立即入队（入队时间==释放时间）。同一流的新实例
  追加在未发送帧之后（FIFO），绝不覆盖未发送帧。
- 完成时刻 <= release+deadline 视为按期；在截止时刻仍未完成即为超期。

等待链归因（wait_chain）：在模拟过程中依托逐时隙状态记录，把某个实例
[释放, 开始发送) 或 [释放, 截止期) 的等待切成按时间连续、互不重叠的区间，
每段给出唯一阻塞类别：
- gate_closed        ：本优先级门未开，关闭门后等待下一窗口；
- window_too_short   ：门已开但窗口剩余不足以完整发送，不启动；
- higher_priority_tx ：更高优先级帧非抢占先占单出口；
- nonpreemptive_hold ：其他帧非抢占占用出口（含跨周期遗留占用）；
- same_flow_fifo     ：同流 FIFO 前序实例未完成，本实例不能超越。
门关闭、窗口不足与高优先级先占是不同机制，相邻但不同类的区间绝不合并。

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
MAX_WAIT_CHAIN = 20000          # 单实例等待链区间上限，控制证据载荷
GROWTH_CONFIRMATIONS = 2        # 超周期边界连续增长两次即确认无法收敛
DRAIN_PROOFS_NEEDED = 2         # 连续两个超周期边界排空才判定可调度

# 等待区间类别（顺序即归因判定优先级，互斥且不合并）。
CAT_GATE_CLOSED = "gate_closed"
CAT_WINDOW_TOO_SHORT = "window_too_short"
CAT_HIGHER_TX = "higher_priority_tx"
CAT_HOLD = "nonpreemptive_hold"
CAT_FIFO = "same_flow_fifo"


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
    # 截止期到来时尚未开始发送（在队等待中超期）：等待证据精确覆盖至截止期。
    overdue_unsent: bool = False

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


class InstanceNotFound(LookupError):
    """等待链查询指向裁决中从未出现（释放）的实例。"""


class Untraceable(RuntimeError):
    """实例已出现，但模拟证据无法连续覆盖其等待区间。"""


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
        self.prio_order = sorted(self.queues)
        self.next_seq: dict[str, int] = {f.flow_id: 0 for f in req.flows}
        self.in_flight: Frame | None = None

        # 所有已释放实例的注册表（fid -> Frame），供等待链查询追溯。
        self.frames: dict[str, Frame] = {}

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
                self.frames[frame.fid] = frame
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
                if frame.status == "queued":
                    # 截止期到来时仍在队等待：首个超期且尚未开始发送，
                    # 其等待证据必须精确覆盖至截止期并标明未发送。
                    frame.overdue_unsent = True
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

    # ---- 等待链归因（基于连续时隙重建） ----

    def _head_of(self, seg: Segment, p: int) -> str | None:
        q = seg.queues.get(str(p), [])
        return q[0] if q else None

    def _top_ready_priority(
        self, seg: Segment, s: int, gate: frozenset[int]
    ) -> int | None:
        """当前时隙真正可启动发送的最高优先级。

        单出口被非抢占占用期间没有任何帧能启动 -> None；空闲时按严格优先级，
        取门开放、位于队首且窗口剩余放得下的最高（数值最小）优先级。
        """
        if seg.transmitting is not None:
            return None
        for p in self.prio_order:
            fid = self._head_of(seg, p)
            if fid is None or p not in gate:
                continue
            remain = self._window_end_at(s, p) - s
            if remain >= self.frames[fid].transmit_time:
                return p
        return None

    def _classify(
        self,
        frame: Frame,
        seg: Segment,
        s: int,
        gate: frozenset[int],
    ) -> tuple[str, str | None, str | None, int | None, str]:
        """返回 (类别, 关联帧, 关联流, 关联帧释放时刻, 说明)。

        归因互斥，顺序与模拟决策一致：启动逻辑只在链路空闲时运行，故一旦有
        其他帧非抢占发送，出口占用即直接约束（高优先级先占 / 同流前序发送 /
        跨周期占用）；链路空闲时再依次判定同流排队、门关闭、窗口不足。
        门关闭、窗口不足与高优先级先占因此永远不会被合并成同一原因。
        """
        tx = seg.transmitting
        own_q = seg.queues.get(str(frame.priority), [])
        pos = own_q.index(frame.fid) if frame.fid in own_q else -1

        def tx_ref() -> tuple[str, str, int | None]:
            src = self.frames.get(tx["frame"])  # type: ignore[index]
            return (
                tx["frame"],  # type: ignore[index]
                tx["flow_id"],  # type: ignore[index]
                src.release if src is not None else None,
            )

        # 1) 出口被其他帧非抢占占用（启动逻辑此时根本不会运行）。
        if tx is not None and tx["frame"] != frame.fid:
            src_fid, src_flow, src_rel = tx_ref()
            if tx["priority"] < frame.priority:  # type: ignore[index]
                return (
                    CAT_HIGHER_TX,
                    src_fid,
                    src_flow,
                    src_rel,
                    f"更高优先级帧 {src_fid}（P{tx['priority']}）非抢占先占单出口",  # type: ignore[index]
                )
            if tx["priority"] == frame.priority:  # type: ignore[index]
                # 优先级在流间唯一：同优先级占用帧即同流前序实例（FIFO）。
                return (
                    CAT_FIFO,
                    src_fid,
                    src_flow,
                    src_rel,
                    f"同流 FIFO 前序帧 {src_fid} 正在非抢占发送，本帧不得超越",
                )
            return (
                CAT_HOLD,
                src_fid,
                src_flow,
                src_rel,
                f"帧 {src_fid} 非抢占占用出口（含跨周期遗留占用），本帧等待其发完",
            )
        # 2) 链路空闲：同流前序实例仍排在本帧之前（FIFO，绝不覆盖/超越）。
        if pos > 0:
            head_fid = own_q[0]
            head = self.frames.get(head_fid)
            return (
                CAT_FIFO,
                head_fid,
                head.flow_id if head is not None else frame.flow_id,
                head.release if head is not None else None,
                f"同流 FIFO：前序帧 {head_fid} 尚未完成，本帧排队其后不得覆盖",
            )
        # 3) 本优先级门关闭：关闭后只能等待下一开放窗口。
        if frame.priority not in gate:
            return (
                CAT_GATE_CLOSED,
                None,
                None,
                None,
                f"优先级 {frame.priority} 的门控项关闭，等待下一开放窗口",
            )
        # 4) 开放窗口剩余不足以完整发送：不启动，继续等待。
        remain = self._window_end_at(s, frame.priority) - s
        if remain < frame.transmit_time:
            return (
                CAT_WINDOW_TOO_SHORT,
                None,
                None,
                None,
                f"门窗口剩余 {remain}us 小于发送时长 {frame.transmit_time}us，"
                "无法完整发送故不启动",
            )
        # 门开、窗口足、链路空闲且本帧为队首，却仍在等待：与模拟语义矛盾，
        # 证据不可追溯，交由上层报明确错误而不是编造原因。
        raise Untraceable(f"{frame.fid} 在 {s}us 满足全部启动条件却仍等待")

    def wait_chain(self, fid: str) -> dict[str, Any]:
        """为已出现实例重建 [释放, 开始发送)（未开始则到截止期）的等待链。

        区间按时间连续、互不重叠；每段带队列长度、门状态、可发送最高优先级、
        阻塞类别及占用/同流 FIFO 关联帧。首个超期且未开始的帧证据精确覆盖
        至截止期并标明未发送。
        """
        frame = self.frames.get(fid)
        if frame is None:
            raise InstanceNotFound(f"实例 {fid} 从未在裁决模拟中释放出现")

        # 截止期到来时尚未开始发送的帧：即便其后被继续服务，等待证据也只
        # 精确覆盖至截止期，并标明“未发送”。
        unsent = frame.overdue_unsent or frame.start is None
        if frame.overdue_unsent:
            wait_end = frame.deadline_abs
        else:
            wait_end = frame.start if frame.start is not None else frame.deadline_abs
        gate_priority = frame.priority

        intervals: list[dict[str, Any]] = []
        covered_from: int | None = None
        covered_to: int | None = None
        cat_totals: dict[str, int] = {}

        for seg in self.segments:
            if seg.end <= frame.release or seg.start >= wait_end:
                continue
            s = max(seg.start, frame.release)
            e = min(seg.end, wait_end)
            if s >= e:
                continue
            gate = frozenset(seg.gate_open)
            own_q = seg.queues.get(str(frame.priority), [])
            if unsent and frame.fid not in own_q and s < frame.deadline_abs:
                # 截止期前未开始的帧必须始终在队；缺失即证据链断裂。
                raise Untraceable(
                    f"{fid} 在 [{s},{e}) 时隙不在 P{frame.priority} 队列中"
                )
            category, src_fid, src_flow, src_rel, detail = self._classify(
                frame, seg, s, gate
            )
            top_ready = self._top_ready_priority(seg, s, gate)
            queue_len = sum(len(q) for q in seg.queues.values())
            position = own_q.index(frame.fid) + 1 if frame.fid in own_q else None
            duration = e - s
            cat_totals[category] = cat_totals.get(category, 0) + duration
            intervals.append(
                {
                    "index": len(intervals),
                    "from": s,
                    "to": e,
                    "duration_us": duration,
                    "queue_length": queue_len,
                    "queue_position": position,
                    "gate_state": "open" if gate_priority in gate else "closed",
                    "gate_open": list(seg.gate_open),
                    "top_ready_priority": top_ready,
                    "blocking_category": category,
                    "source_frame": src_fid,
                    "source_flow": src_flow,
                    "source_release": src_rel,
                    "detail": detail,
                }
            )
            covered_from = s if covered_from is None else min(covered_from, s)
            covered_to = e if covered_to is None else max(covered_to, e)

        if not unsent and frame.start == frame.release:
            # 释放即获得出口：等待为空，仍返回结构完整、连续无重叠的空链。
            return {
                "frame": self._frame_block(frame, False),
                "coverage": [frame.release, wait_end],
                "coverage_note": "实例释放时刻即开始发送，等待为空",
                "total_wait_us": 0,
                "duration_by_category": {},
                "intervals": [],
            }
        if not intervals or covered_from != frame.release or covered_to != wait_end:
            got = (
                "无任何时隙覆盖"
                if covered_from is None
                else f"实际覆盖 [{covered_from},{covered_to})"
            )
            raise Untraceable(
                f"{fid} 等待证据无法连续覆盖 [{frame.release},{wait_end})，{got}"
            )
        # 连续性与无重叠自检：区间由连续时隙裁剪而来，首尾必须严格相接。
        for a, b in zip(intervals, intervals[1:]):
            if a["to"] != b["from"]:
                raise Untraceable(
                    f"{fid} 等待链在 {a['to']}us 处不连续（下段起 {b['from']}）"
                )

        return {
            "frame": self._frame_block(frame, unsent),
            "coverage": [frame.release, wait_end],
            "coverage_note": (
                "证据自释放连续覆盖至截止期；该帧截止期前始终未开始发送"
                if unsent
                else "证据自释放连续覆盖至实际开始发送时刻"
            ),
            "total_wait_us": wait_end - frame.release,
            "duration_by_category": cat_totals,
            "intervals": intervals,
        }

    def _frame_block(self, frame: Frame, unsent: bool) -> dict[str, Any]:
        """等待链中的实例口径：未在截止期前开始发送时起止一律记 None。"""
        if unsent:
            state = (
                "overdue_waiting_unsent" if frame.overdue_unsent
                else "waiting_unsent"
            )
            return {
                "frame": frame.fid,
                "flow_id": frame.flow_id,
                "priority": frame.priority,
                "seq": frame.seq,
                "release": frame.release,
                "enqueue": frame.release,
                "deadline": frame.deadline_abs,
                "transmit_start": None,
                "transmit_end": None,
                "state": state,
                "unsent": True,
            }
        return {
            "frame": frame.fid,
            "flow_id": frame.flow_id,
            "priority": frame.priority,
            "seq": frame.seq,
            "release": frame.release,
            "enqueue": frame.release,
            "deadline": frame.deadline_abs,
            "transmit_start": frame.start,
            "transmit_end": frame.finish,
            "state": "sent" if frame.status == "sent" else frame.status,
            "unsent": False,
        }

    def instance_catalog(self, limit: int = MAX_WAIT_CHAIN) -> dict[str, Any]:
        """枚举模拟中已出现（释放）的流实例，供页面选择与接口校验。

        queryable 表示其等待区间完整落在已记录的连续时隙证据内，
        等待链接口可给出连续覆盖；否则接口将返回不可追溯错误而非编造。
        """
        trace_end = self.segments[-1].end if self.segments else 0
        items: list[dict[str, Any]] = []
        truncated = False
        for fid in sorted(self.frames, key=lambda x: (self.frames[x].release, x)):
            fr = self.frames[fid]
            if fr.release > trace_end:
                continue
            if len(items) >= limit:
                truncated = True
                break
            if fr.overdue_unsent:
                wait_end = fr.deadline_abs
            else:
                wait_end = fr.start if fr.start is not None else fr.deadline_abs
            items.append(
                {
                    "frame": fid,
                    "flow_id": fr.flow_id,
                    "priority": fr.priority,
                    "seq": fr.seq,
                    "release": fr.release,
                    "deadline": fr.deadline_abs,
                    "transmit_start": fr.start,
                    "transmit_end": fr.finish,
                    "status": fr.status,
                    "unsent": fr.start is None,
                    "queryable": wait_end <= trace_end,
                }
            )
        return {
            "instances": items,
            "truncated": truncated,
            "total": len(self.frames),
            "trace_end": trace_end,
        }

    def _legacy_blockers(self, chain: dict[str, Any]) -> list[dict[str, Any]]:
        """由等待链区间派生旧版 blockers 视图（仅合并相邻同类同源段）。"""
        merged: list[dict[str, Any]] = []
        for iv in chain["intervals"]:
            if (
                merged
                and merged[-1]["type"] == iv["blocking_category"]
                and merged[-1]["to"] == iv["from"]
                and merged[-1].get("source_frame") == iv["source_frame"]
            ):
                merged[-1]["to"] = iv["to"]
                continue
            merged.append(
                {
                    "type": iv["blocking_category"],
                    "from": iv["from"],
                    "to": iv["to"],
                    "source_frame": iv["source_frame"],
                    "source_flow": iv["source_flow"],
                    "source_priority": (
                        self.frames[iv["source_frame"]].priority
                        if iv["source_frame"] is not None
                        and iv["source_frame"] in self.frames
                        else None
                    ),
                    "detail": iv["detail"],
                }
            )
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
            "instances": self.instance_catalog(),
        }

    def _verdict_miss(self, frame: Frame, at_t: int) -> dict[str, Any]:
        # 截止期时刻是否已开始发送：overdue_unsent 记录的是“截止期到来时仍在
        # 队等待、从未开始”。即便该帧之后被继续服务，超期证据也以截止期时刻
        # 为准，精确覆盖至截止期并标明未发送。
        unsent_at_deadline = frame.overdue_unsent
        started_then_late = frame.start is not None and not unsent_at_deadline
        # 等待链按模拟时记录的连续时隙重建：已开始则覆盖到开始发送时刻，
        # 未开始则精确覆盖至截止期并标明未发送。
        chain = self.wait_chain(frame.fid)
        blockers = self._legacy_blockers(chain)
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
                "transmit_start": None if unsent_at_deadline else frame.start,
                "transmit_end": frame.finish if started_then_late else None,
                "deadline": frame.deadline_abs,
                "detected_at": frame.deadline_abs,
                "state_when_overdue": "waiting" if unsent_at_deadline else "transmitting",
                "unsent": unsent_at_deadline,
                "note": (
                    "帧在截止时刻仍未获得完整发送窗口、尚未开始发送"
                    if unsent_at_deadline
                    else "帧已开始发送但非抢占发送越过截止期"
                ),
                "blockers": blockers,
                "wait_chain": chain,
            },
            "instances": self.instance_catalog(),
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
                    "unsent": fr.overdue_unsent or fr.start is None,
                }
                for fr in self.late_frames
            ],
            "events": self.events,
            "timeline": self._timeline(end_t),
            "instances": self.instance_catalog(),
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
