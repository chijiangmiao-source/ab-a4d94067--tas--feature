"""调度引擎单元测试。"""

import unittest

from app.models import ValidationError, parse_request
from app.scheduler import adjudicate, build_wait_chain


def make(audit_id="T", gate_period=1000, flows=None, gates=None):
    return parse_request(
        {
            "audit_id": audit_id,
            "gate_period": gate_period,
            "flows": flows or [],
            "gate_entries": gates or [],
        }
    )


# 一组跨周期遗留但最终收敛的标准场景：
# G=1000；HI(p0)/LO(p1) 周期均 2000、发送 400；门只在每周期前 400us 开放。
# 严格优先级下 HI 在偶数周期占满窗口，LO 遗留到下一周期发送，
# 在每个 2000us 超周期边界队列排空。
CARRYOVER = dict(
    gate_period=1000,
    flows=[
        {"flow_id": "HI", "priority": 0, "period": 2000, "transmit_time": 400, "deadline": 2000},
        {"flow_id": "LO", "priority": 1, "period": 2000, "transmit_time": 400, "deadline": 2000},
    ],
    gates=[{"start": 0, "end": 400, "priorities": [0, 1]}],
)

GROWTH = dict(
    gate_period=1000,
    flows=[
        {"flow_id": "FLOOD", "priority": 0, "period": 1000, "transmit_time": 800, "deadline": 1000}
    ],
    gates=[{"start": 0, "end": 500, "priorities": [0]}],
)

# LO 可在 600..1000 窗口完整发送（400>=300），但被 HI 挤到 600 才开始，
# 900 才发完，超过 800 的截止期；队列随后排空，属于超期而非不收敛。
MISS = dict(
    gate_period=1000,
    flows=[
        {"flow_id": "HI", "priority": 0, "period": 1000, "transmit_time": 600, "deadline": 1000},
        {"flow_id": "LO", "priority": 1, "period": 2000, "transmit_time": 300, "deadline": 800},
    ],
    gates=[
        {"start": 0, "end": 600, "priorities": [0]},
        {"start": 600, "end": 1000, "priorities": [1]},
    ],
)


class ValidationTests(unittest.TestCase):
    def test_valid(self):
        req = make(flows=CARRYOVER["flows"], gates=CARRYOVER["gates"])
        self.assertEqual(req.gate_period, 1000)

    def test_bad_audit(self):
        with self.assertRaises(ValidationError):
            make(audit_id="  ", flows=CARRYOVER["flows"], gates=CARRYOVER["gates"])

    def test_flow_count_limit(self):
        flows = [
            {"flow_id": f"F{i}", "priority": i, "period": 1000,
             "transmit_time": 10, "deadline": 1000}
            for i in range(9)
        ]
        with self.assertRaises(ValidationError):
            make(flows=flows, gates=[{"start": 0, "end": 1000, "priorities": list(range(9))[:8]}])

    def test_duplicate_priority(self):
        with self.assertRaises(ValidationError):
            make(
                flows=[
                    {"flow_id": "A", "priority": 0, "period": 1000, "transmit_time": 10, "deadline": 1000},
                    {"flow_id": "B", "priority": 0, "period": 1000, "transmit_time": 10, "deadline": 1000},
                ],
                gates=[{"start": 0, "end": 1000, "priorities": [0]}],
            )

    def test_gate_not_sorted(self):
        with self.assertRaises(ValidationError):
            make(
                flows=[{"flow_id": "A", "priority": 0, "period": 1000,
                        "transmit_time": 10, "deadline": 1000}],
                gates=[
                    {"start": 500, "end": 600, "priorities": [0]},
                    {"start": 0, "end": 100, "priorities": [0]},
                ],
            )

    def test_gate_out_of_range(self):
        with self.assertRaises(ValidationError):
            make(
                flows=[{"flow_id": "A", "priority": 0, "period": 1000,
                        "transmit_time": 10, "deadline": 1000}],
                gates=[{"start": 0, "end": 1001, "priorities": [0]}],
            )

    def test_overlapping_same_priority(self):
        with self.assertRaises(ValidationError):
            make(
                flows=[{"flow_id": "A", "priority": 0, "period": 1000,
                        "transmit_time": 10, "deadline": 1000}],
                gates=[
                    {"start": 0, "end": 500, "priorities": [0]},
                    {"start": 400, "end": 600, "priorities": [0]},
                ],
            )

    def test_unknown_priority_referenced(self):
        with self.assertRaises(ValidationError):
            make(
                flows=[{"flow_id": "A", "priority": 0, "period": 1000,
                        "transmit_time": 10, "deadline": 1000}],
                gates=[{"start": 0, "end": 500, "priorities": [3]}],
            )

    def test_deadline_shorter_than_tx(self):
        with self.assertRaises(ValidationError):
            make(
                flows=[{"flow_id": "A", "priority": 0, "period": 1000,
                        "transmit_time": 200, "deadline": 100}],
                gates=[{"start": 0, "end": 1000, "priorities": [0]}],
            )


class SchedulableTests(unittest.TestCase):
    def test_carryover_converges_with_drain_proofs(self):
        v = adjudicate(make(**CARRYOVER))
        self.assertEqual(v["verdict"], "SCHEDULABLE")
        self.assertEqual(v["hyperperiod"], 2000)
        snaps = v["cycle_snapshots"]
        # 证据窗口覆盖两个超周期：边界 t=0,1000,2000,3000,4000。
        by_t = {s["t"]: s["pending"] for s in snaps}
        self.assertEqual(by_t[0], 0)
        self.assertEqual(by_t[1000], 1)   # LO#0 跨周期遗留
        self.assertEqual(by_t[2000], 0)   # 超周期边界排空
        self.assertEqual(by_t[3000], 1)
        self.assertEqual(by_t[4000], 0)
        for flow, rows in v["flow_evidence"].items():
            self.assertTrue(rows, f"{flow} 应有周期证据")
            self.assertTrue(all(r["on_time"] for r in rows))

    def test_evidence_contains_release_enqueue_start_finish(self):
        v = adjudicate(make(**CARRYOVER))
        lo = v["flow_evidence"]["LO"][0]
        self.assertEqual((lo["release"], lo["enqueue"]), (0, 0))
        self.assertEqual(lo["start"], 1000)   # 被 HI 挤到下一周期
        self.assertEqual(lo["finish"], 1400)
        self.assertLessEqual(lo["finish"], lo["deadline"])

    def test_fifo_no_overwrite(self):
        # 窗口极短，两帧在队中按 FIFO 共存，新释放不得覆盖旧帧。
        v = adjudicate(
            make(
                gate_period=1000,
                flows=[{"flow_id": "X", "priority": 0, "period": 1000,
                        "transmit_time": 800, "deadline": 1000}],
                gates=[{"start": 0, "end": 500, "priorities": [0]}],
            )
        )
        self.assertEqual(v["verdict"], "NON_CONVERGENT")
        # 时间线中必须能看到 X#0 与 X#1 同时排队（FIFO，未被覆盖）。
        found = False
        for seg in v["timeline"]:
            if seg["queues"].get("0") == ["X#0", "X#1"]:
                found = True
        self.assertTrue(found, "同流新帧必须追加而非覆盖未发送帧")


class DeadlineMissTests(unittest.TestCase):
    def test_miss_fields_and_blocker(self):
        v = adjudicate(make(audit_id="M", **MISS))
        self.assertEqual(v["verdict"], "DEADLINE_MISS")
        f = v["first_overdue_frame"]
        self.assertEqual(f["frame"], "LO#0")
        self.assertEqual(f["release"], 0)
        self.assertEqual(f["enqueue"], 0)
        self.assertEqual(f["transmit_start"], 600)
        self.assertEqual(f["transmit_end"], 900)
        self.assertEqual(f["deadline"], 800)
        self.assertEqual(f["state_when_overdue"], "transmitting")
        kinds = [b["type"] for b in f["blockers"]]
        self.assertIn("higher_priority_tx", kinds)
        hp = next(b for b in f["blockers"] if b["type"] == "higher_priority_tx")
        self.assertEqual((hp["from"], hp["to"]), (0, 600))
        self.assertEqual(hp["source_frame"], "HI#0")

    def test_window_too_short_waits(self):
        # 首窗口对 p1 仅开放 100us（<300us，放不下 -> 等待），随后门关闭，
        # 再被 HI 占用，LO 直到 600us 才开始、900us 发完而截止期 800us；
        # 队列随后排空，故判 DEADLINE_MISS，阻塞含 window_too_short。
        v = adjudicate(
            make(
                gate_period=1000,
                flows=[
                    {"flow_id": "HI", "priority": 0, "period": 2000,
                     "transmit_time": 500, "deadline": 2000},
                    {"flow_id": "LO", "priority": 1, "period": 2000,
                     "transmit_time": 300, "deadline": 800},
                ],
                gates=[
                    {"start": 0, "end": 100, "priorities": [1]},
                    {"start": 100, "end": 600, "priorities": [0]},
                    {"start": 600, "end": 1000, "priorities": [1]},
                ],
            )
        )
        self.assertEqual(v["verdict"], "DEADLINE_MISS")
        f = v["first_overdue_frame"]
        self.assertEqual(f["frame"], "LO#0")
        self.assertEqual(f["transmit_start"], 600)
        self.assertIn("window_too_short", [b["type"] for b in f["blockers"]])

    def test_never_fitting_window_grows_and_rejected(self):
        # 窗口结构性短于发送时长 -> 没有任何帧能发送，遗留持续增长 -> 拒绝。
        v = adjudicate(
            make(
                gate_period=1000,
                flows=[{"flow_id": "L", "priority": 1, "period": 2000,
                        "transmit_time": 500, "deadline": 1500}],
                gates=[{"start": 0, "end": 300, "priorities": [1]}],
            )
        )
        self.assertEqual(v["verdict"], "NON_CONVERGENT")
        chain = [s["pending"] for s in v["growth_chain"]]
        self.assertTrue(all(b > a for a, b in zip(chain, chain[1:])))


class NonConvergentTests(unittest.TestCase):
    def test_growth_rejected_with_increasing_chain(self):
        v = adjudicate(make(**GROWTH))
        self.assertEqual(v["verdict"], "NON_CONVERGENT")
        chain = v["growth_chain"]
        self.assertGreaterEqual(len(chain), 2)
        pendings = [s["pending"] for s in chain]
        self.assertEqual(pendings, sorted(pendings))
        self.assertTrue(all(b > a for a, b in zip(pendings, pendings[1:])))

    def test_unlisted_priority_never_transmits(self):
        # 门控从未列名 p1 -> 该流每周期释放却永远得不到发送，
        # 遗留队列逐周期增长 -> NON_CONVERGENT，且时间线中 B 从未发送。
        v = adjudicate(
            make(
                gate_period=1000,
                flows=[
                    {"flow_id": "A", "priority": 0, "period": 1000,
                     "transmit_time": 100, "deadline": 1000},
                    {"flow_id": "B", "priority": 1, "period": 1000,
                     "transmit_time": 100, "deadline": 1000},
                ],
                gates=[{"start": 0, "end": 200, "priorities": [0]}],
            )
        )
        self.assertEqual(v["verdict"], "NON_CONVERGENT")
        pendings = [s["pending"] for s in v["queue_growth"]]
        self.assertEqual(pendings, sorted(pendings))
        self.assertGreaterEqual(len(v["growth_chain"]), 2)
        for seg in v["timeline"]:
            if seg["transmitting"] is not None:
                self.assertNotEqual(seg["transmitting"]["flow_id"], "B")


class WaitChainTests(unittest.TestCase):
    """逐段等待链：模拟时记录的阻塞归因（类别互斥、区间连续无重叠）。"""

    def test_carryover_lo0_higher_priority_then_gate_closed(self):
        v = adjudicate(make(**CARRYOVER))
        waits = v["wait_chains"]
        self.assertTrue(waits["available"])
        payload, err = build_wait_chain(waits, "LO", 0)
        self.assertIsNone(err)
        w = payload["wait"]
        # LO#0 被 HI#0 占用 [0,400)，随后门全关等到 1000 才开始发送。
        self.assertEqual((w["from"], w["to"], w["total"]), (0, 1000, 1000))
        self.assertEqual(w["outcome"], "started")
        iv = w["intervals"]
        self.assertEqual([(s["from"], s["to"]) for s in iv], [(0, 400), (400, 1000)])
        self.assertEqual(iv[0]["blocker"], "higher_priority_tx")
        self.assertEqual(
            iv[0]["source"],
            {"frame": "HI#0", "flow_id": "HI", "release": 0},
        )
        self.assertEqual(iv[0]["queue_length"], 1)
        self.assertEqual(iv[0]["gate_open"], [0, 1])
        self.assertEqual(iv[0]["eligible_priority"], 1)
        self.assertEqual(iv[1]["blocker"], "gate_closed")
        self.assertIsNone(iv[1]["source"])
        self.assertEqual(iv[1]["gate_open"], [])
        self.assertIsNone(iv[1]["eligible_priority"])

    def test_window_too_short_not_merged_with_other_causes(self):
        # 同一帧的等待依次由 窗口不足 -> 高优先级占用 造成，两类必须分段。
        v = adjudicate(
            make(
                gate_period=1000,
                flows=[
                    {"flow_id": "HI", "priority": 0, "period": 2000,
                     "transmit_time": 500, "deadline": 2000},
                    {"flow_id": "LO", "priority": 1, "period": 2000,
                     "transmit_time": 300, "deadline": 800},
                ],
                gates=[
                    {"start": 0, "end": 100, "priorities": [1]},
                    {"start": 100, "end": 600, "priorities": [0]},
                    {"start": 600, "end": 1000, "priorities": [1]},
                ],
            )
        )
        payload, err = build_wait_chain(v["wait_chains"], "LO", 0)
        self.assertIsNone(err)
        kinds = [(s["from"], s["to"], s["blocker"]) for s in payload["wait"]["intervals"]]
        self.assertEqual(
            kinds,
            [(0, 100, "window_too_short"), (100, 600, "higher_priority_tx")],
        )

    def test_same_flow_fifo_records_source_frame(self):
        # LO#1 释放时同流 LO#0 正在发送：阻塞类别为同流 FIFO，关联帧 LO#0。
        # （前序帧被高优先级推迟后发送越过截止期，故整体裁决为 DEADLINE_MISS；
        # 同流 FIFO 等待本身独立于裁决类型记录。）
        v = adjudicate(
            make(
                gate_period=1000,
                flows=[
                    {"flow_id": "HI", "priority": 0, "period": 2000,
                     "transmit_time": 400, "deadline": 2000},
                    {"flow_id": "LO", "priority": 1, "period": 500,
                     "transmit_time": 300, "deadline": 500},
                ],
                gates=[
                    {"start": 0, "end": 400, "priorities": [0]},
                    {"start": 400, "end": 1000, "priorities": [0, 1]},
                ],
            )
        )
        self.assertEqual(v["verdict"], "DEADLINE_MISS")
        payload, err = build_wait_chain(v["wait_chains"], "LO", 1)
        self.assertIsNone(err)
        iv = payload["wait"]["intervals"]
        self.assertEqual([(s["from"], s["to"]) for s in iv], [(500, 700)])
        self.assertEqual(iv[0]["blocker"], "same_flow_fifo")
        self.assertEqual(iv[0]["source"]["frame"], "LO#0")
        self.assertEqual(iv[0]["source"]["flow_id"], "LO")
        self.assertEqual(iv[0]["source"]["release"], 0)

    def test_unsent_frame_covers_exactly_to_deadline(self):
        # 门从不为 p1 开放：B#0 到截止期仍未开始，证据精确覆盖至截止期。
        v = adjudicate(
            make(
                gate_period=1000,
                flows=[
                    {"flow_id": "A", "priority": 0, "period": 1000,
                     "transmit_time": 100, "deadline": 1000},
                    {"flow_id": "B", "priority": 1, "period": 1000,
                     "transmit_time": 100, "deadline": 1000},
                ],
                gates=[{"start": 0, "end": 200, "priorities": [0]}],
            )
        )
        self.assertEqual(v["verdict"], "NON_CONVERGENT")
        payload, err = build_wait_chain(v["wait_chains"], "B", 0)
        self.assertIsNone(err)
        w = payload["wait"]
        self.assertEqual((w["from"], w["to"]), (0, 1000))
        self.assertEqual(w["outcome"], "unsent")
        self.assertFalse(payload["frame"]["started"])
        self.assertTrue(payload["frame"]["unsent_at_deadline"])
        iv = w["intervals"]
        self.assertEqual(iv[0]["blocker"], "higher_priority_tx")  # [0,200) A#0 占用
        self.assertIn("gate_closed", [s["blocker"] for s in iv])
        self.assertEqual(iv[-1]["to"], 1000)

    def test_missing_instance_returns_error(self):
        v = adjudicate(make(**CARRYOVER))
        payload, err = build_wait_chain(v["wait_chains"], "LO", 99)
        self.assertIsNone(payload)
        self.assertEqual(err[0], 404)
        self.assertEqual(err[1], "INSTANCE_NOT_FOUND")
        payload, err = build_wait_chain(v["wait_chains"], "NOPE", 0)
        self.assertIsNone(payload)
        self.assertEqual(err[0], 404)

    def test_intervals_contiguous_for_every_instance(self):
        # 任意实例：区间连续无重叠，且精确覆盖 [release, start/deadline/末端]。
        v = adjudicate(make(**CARRYOVER))
        waits = v["wait_chains"]
        for meta in waits["frames"].values():
            payload, err = build_wait_chain(waits, meta["flow_id"], meta["seq"])
            self.assertIsNone(err, meta)
            w = payload["wait"]
            iv = w["intervals"]
            if not iv:
                self.assertEqual(w["total"], 0)
                continue
            self.assertEqual(iv[0]["from"], w["from"])
            self.assertEqual(iv[-1]["to"], w["to"])
            for a, b in zip(iv, iv[1:]):
                self.assertLess(a["from"], a["to"])
                self.assertEqual(a["to"], b["from"])
            self.assertEqual(
                sum(s["to"] - s["from"] for s in iv), w["total"]
            )

    def test_instances_index_in_verdict(self):
        v = adjudicate(make(**CARRYOVER))
        self.assertIn("instances", v)
        fids = {f["frame"] for f in v["instances"]}
        self.assertIn("LO#0", fids)
        self.assertIn("HI#0", fids)
        self.assertEqual(v["instances_total"], len(v["instances"]))
        lo0 = next(f for f in v["instances"] if f["frame"] == "LO#0")
        self.assertEqual(
            {k: lo0[k] for k in ("release", "deadline", "start", "finish")},
            {"release": 0, "deadline": 2000, "start": 1000, "finish": 1400},
        )


class TimelineTests(unittest.TestCase):
    def test_slots_cover_spans_without_gaps(self):
        v = adjudicate(make(**CARRYOVER))
        tl = v["timeline"]
        self.assertTrue(tl)
        for a, b in zip(tl, tl[1:]):
            self.assertEqual(a["end"], b["start"])
        # 每个时隙都带门状态、队列与发送结果三要素。
        for seg in tl:
            self.assertIn("gate_open", seg)
            self.assertIn("queues", seg)
            self.assertIn("transmitting", seg)

    def test_nonpreemptive_completes_past_gate_close_inside_window(self):
        # 帧恰好在窗口关闭点发完：非抢占占满，之后门关闭也不影响。
        v = adjudicate(
            make(
                gate_period=1000,
                flows=[{"flow_id": "A", "priority": 0, "period": 1000,
                        "transmit_time": 500, "deadline": 1000}],
                gates=[{"start": 0, "end": 500, "priorities": [0]}],
            )
        )
        self.assertEqual(v["verdict"], "SCHEDULABLE")
        self.assertEqual(v["flow_evidence"]["A"][0]["finish"], 500)


if __name__ == "__main__":
    unittest.main()
