#!/usr/bin/env python3
"""审计接口 HTTP 冒烟验收。

用法：
  python3 scripts/http_smoke.py [BASE_URL]

- 不传 BASE_URL：本脚本自行在随机端口启动真实 HTTP 服务再测；
- 传入 BASE_URL（如 http://web:8080）：对已运行服务做真接口冒烟。

验收覆盖：
1. 健康检查与静态页面；
2. 跨周期遗留帧最终收敛：提交可调度场景，读取冻结裁决，
   校验 SCHEDULABLE、逐时隙证据与遗留边界样本（t=1000 时遗留 1 帧、超周期排空）；
3. 幂等冻结：相同内容重复提交/读取返回同一 content_hash；
4. 冲突：同一审计标识不同内容 -> 409 且原裁决不变；
5. 队列增长拒绝：提交不可收敛场景，校验 NON_CONVERGENT 与连续增长链；
6. 输入校验失败返回 400；
7. 实例等待链：区间连续无重叠、阻塞类别分段不合并、关联帧流与释放时刻、
   不存在实例 404、查询后冻结结果不变。
任一断言失败以非零退出码退出。
"""

from __future__ import annotations

import json
import os
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.request

HERE = os.path.dirname(os.path.abspath(__file__))
ROOT = os.path.dirname(HERE)

CARRYOVER = {
    "audit_id": "SMK-CARRY-001",
    "gate_period": 1000,
    "flows": [
        {"flow_id": "HI", "priority": 0, "period": 2000,
         "transmit_time": 400, "deadline": 2000},
        {"flow_id": "LO", "priority": 1, "period": 2000,
         "transmit_time": 400, "deadline": 2000},
    ],
    "gate_entries": [{"start": 0, "end": 400, "priorities": [0, 1]}],
}

GROWTH = {
    "audit_id": "SMK-GROWTH-001",
    "gate_period": 1000,
    "flows": [
        {"flow_id": "FLOOD", "priority": 0, "period": 1000,
         "transmit_time": 800, "deadline": 1000}
    ],
    "gate_entries": [{"start": 0, "end": 500, "priorities": [0]}],
}

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    mark = "PASS" if cond else "FAIL"
    print(f"  [{mark}] {name}" + (f" — {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def request(method: str, url: str, payload: dict | None = None) -> tuple[int, dict | str]:
    data = json.dumps(payload).encode("utf-8") if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method)
    if data is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            raw = resp.read().decode("utf-8")
            ctype = resp.headers.get("Content-Type", "")
            return resp.status, (json.loads(raw) if "json" in ctype else raw)
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8")
        try:
            return exc.code, json.loads(raw)
        except json.JSONDecodeError:
            return exc.code, raw


def wait_ready(base: str, proc: subprocess.Popen | None = None) -> None:
    for _ in range(60):
        if proc is not None and proc.poll() is not None:
            raise RuntimeError("验收服务进程提前退出")
        try:
            status, body = request("GET", base + "/api/health")
            if status == 200:
                return
        except OSError:
            pass
        time.sleep(0.25)
    raise RuntimeError(f"服务未在预期时间内就绪: {base}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def run_smoke(base: str) -> None:
    print(f"== HTTP 冒烟目标: {base} ==")

    print("[1] 健康检查与静态页面")
    status, body = request("GET", base + "/api/health")
    check("GET /api/health -> 200", status == 200 and isinstance(body, dict) and body.get("ok"))
    status, page = request("GET", base + "/")
    check("GET / -> 200 且含页面标题", status == 200 and "冻结裁决" in str(page))

    print("[2] 跨周期遗留帧收敛证据（SCHEDULABLE）")
    status, body = request("POST", base + "/api/submit", CARRYOVER)
    check("提交可调度场景 -> 201", status == 201, f"status={status} body={body}")
    assert isinstance(body, dict)
    check("裁决为 SCHEDULABLE", body["decision"]["verdict"] == "SCHEDULABLE")
    check("内容哈希已冻结", bool(body.get("content_hash")) and body.get("frozen") is True)
    frozen_hash = body["content_hash"]
    snaps = {s["t"]: s["pending"] for s in body["decision"]["cycle_snapshots"]}
    check("t=1000 边界遗留 1 帧（跨周期队列）", snaps.get(1000) == 1, str(snaps))
    check("t=2000 超周期边界排空", snaps.get(2000) == 0, str(snaps))
    check("连续两个超周期排空", snaps.get(4000) == 0, str(snaps))
    lo_rows = body["decision"]["flow_evidence"]["LO"]
    check("LO 流存在周期发送证据", len(lo_rows) >= 2)
    check("LO#0 在 1000us 才开始（被高优先级遗留推迟）",
          lo_rows[0]["start"] == 1000 and lo_rows[0]["finish"] == 1400,
          str(lo_rows[0]))
    check("全部按期", all(r["on_time"] for r in lo_rows))
    timeline = body["decision"]["timeline"]
    check("逐时隙时间线非空且含门状态/队列/发送结果",
          len(timeline) > 0 and all(
              {"gate_open", "queues", "transmitting"} <= set(seg) for seg in timeline))

    print("[3] 相同提交与读取返回同一冻结结论")
    status, body2 = request("POST", base + "/api/submit", CARRYOVER)
    check("重复提交 -> 200 created=false", status == 200 and body2.get("created") is False)
    check("重复提交哈希一致", body2["content_hash"] == frozen_hash)
    status, body3 = request("GET", base + "/api/decisions/SMK-CARRY-001")
    check("读取冻结裁决 -> 200", status == 200)
    check("读取返回同一哈希", isinstance(body3, dict) and body3["content_hash"] == frozen_hash)
    check("读取结论与提交一致",
          body3["decision"]["verdict"] == "SCHEDULABLE")

    print("[4] 同标识不同内容 -> 409 冲突且原裁决不变")
    changed = json.loads(json.dumps(CARRYOVER))
    changed["flows"][1]["transmit_time"] = 401
    status, body4 = request("POST", base + "/api/submit", changed)
    check("冲突提交 -> 409", status == 409, f"status={status}")
    check("返回冲突错误码", isinstance(body4, dict) and body4.get("error") == "AUDIT_CONFLICT")
    check("冲突响应回显原冻结裁决",
          isinstance(body4, dict)
          and (body4.get("existing_decision") or {}).get("content_hash") == frozen_hash)
    status, body5 = request("GET", base + "/api/decisions/SMK-CARRY-001")
    check("原裁决保持不变", body5["content_hash"] == frozen_hash
          and body5["decision"]["verdict"] == "SCHEDULABLE"
          and body5["request"]["flows"][1]["transmit_time"] == 400)

    print("[5] 队列增长拒绝（NON_CONVERGENT，不截断判通过）")
    status, body6 = request("POST", base + "/api/submit", GROWTH)
    check("提交不可收敛场景 -> 201", status == 201, str(body6))
    d6 = body6["decision"]
    check("裁决为 NON_CONVERGENT", d6["verdict"] == "NON_CONVERGENT")
    chain = [s["pending"] for s in d6["growth_chain"]]
    check("连续增长链至少 2 个边界且严格递增",
          len(chain) >= 2 and all(b > a for a, b in zip(chain, chain[1:])),
          str(chain))
    fifo = any(seg["queues"].get("0", []) == ["FLOOD#0", "FLOOD#1"]
               for seg in d6["timeline"])
    check("时间线证明新帧不覆盖旧帧（FLOOD#0、FLOOD#1 共存）", fifo)

    print("[6] 非法输入 -> 400")
    bad = json.loads(json.dumps(CARRYOVER))
    bad["audit_id"] = "SMK-BAD-001"
    bad["gate_entries"] = [{"start": 900, "end": 100, "priorities": [0]}]
    status, body7 = request("POST", base + "/api/submit", bad)
    check("门控项未按时间/范围 -> 400", status == 400
          and isinstance(body7, dict) and body7.get("error") == "VALIDATION_FAILED")
    status, body8 = request("GET", base + "/api/decisions/NO-SUCH-ID")
    check("读取不存在裁决 -> 404", status == 404)

    print("[7] 实例等待链（按时间连续、无重叠的逐段归因）")
    status, w = request("GET", base + "/api/decisions/SMK-CARRY-001/waits/LO/0")
    check("查询 LO#0 等待链 -> 200", status == 200, f"status={status} body={w}")
    if status == 200 and isinstance(w, dict):
        check("等待链随原冻结裁决返回（哈希一致）", w.get("content_hash") == frozen_hash)
        wait = w["wait"]
        intervals = wait["intervals"]
        contiguous = all(a["to"] == b["from"] for a, b in zip(intervals, intervals[1:]))
        check("等待区间连续无重叠",
              contiguous and intervals[0]["from"] == wait["from"]
              and intervals[-1]["to"] == wait["to"],
              str(intervals))
        check("LO#0 等待覆盖 [0, 1000) 至开始发送",
              wait["from"] == 0 and wait["to"] == 1000, str(wait))
        kinds = [s["blocker"] for s in intervals]
        check("高优先级占用与门关闭分段呈现、不合并",
          "higher_priority_tx" in kinds and "gate_closed" in kinds, str(kinds))
        hp = next(s for s in intervals if s["blocker"] == "higher_priority_tx")
        check("占用段标明关联帧的流与释放时刻",
              hp["source"]["frame"] == "HI#0" and hp["source"]["flow_id"] == "HI"
              and hp["source"]["release"] == 0, str(hp))
        check("每段含队列长度/门状态/可发送最高优先级",
              all({"queue_length", "gate_open", "eligible_priority"} <= set(s)
                  for s in intervals))
    status, w2 = request("GET", base + "/api/decisions/SMK-CARRY-001/waits/LO/99")
    check("不存在的实例 -> 404 INSTANCE_NOT_FOUND",
          status == 404 and isinstance(w2, dict)
          and w2.get("error") == "INSTANCE_NOT_FOUND")
    status, w3 = request("GET", base + "/api/decisions/NO-SUCH-ID/waits/LO/0")
    check("不存在的裁决 -> 404", status == 404)
    status, after = request("GET", base + "/api/decisions/SMK-CARRY-001")
    check("等待链查询后冻结裁决不变",
          isinstance(after, dict) and after.get("content_hash") == frozen_hash
          and after["decision"]["verdict"] == "SCHEDULABLE")


def main() -> int:
    base = sys.argv[1] if len(sys.argv) > 1 else ""
    proc: subprocess.Popen | None = None
    try:
        if not base:
            port = free_port()
            base = f"http://127.0.0.1:{port}"
            proc = subprocess.Popen(
                [sys.executable, "-m", "app.server", "--host", "127.0.0.1", "--port", str(port)],
                cwd=ROOT,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.DEVNULL,
            )
        wait_ready(base, proc)
        run_smoke(base)
    finally:
        if proc is not None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    print()
    if failures:
        print(f"HTTP 冒烟失败：{len(failures)} 项 -> {failures}")
        return 1
    print("HTTP 冒烟全部通过。")
    return 0


if __name__ == "__main__":
    sys.exit(main())
