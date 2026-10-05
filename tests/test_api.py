"""HTTP 接口层测试：在随机端口启动真实服务进行端到端验证。"""

import json
import socket
import threading
import unittest
import urllib.error
import urllib.request
from http.server import ThreadingHTTPServer

from app.server import make_handler
from app.store import DecisionStore


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class HttpServerFixture:
    def __init__(self) -> None:
        self.store = DecisionStore()
        self.httpd = ThreadingHTTPServer(
            ("127.0.0.1", free_port()), make_handler(self.store)
        )
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    @property
    def base(self) -> str:
        host, port = self.httpd.server_address[:2]
        return f"http://{host}:{port}"

    def __enter__(self) -> "HttpServerFixture":
        self.thread.start()
        return self

    def __exit__(self, *exc: object) -> None:
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=5)

    def call(self, method: str, path: str, payload: dict | None = None):
        data = json.dumps(payload).encode() if payload is not None else None
        req = urllib.request.Request(self.base + path, data=data, method=method)
        if data is not None:
            req.add_header("Content-Type", "application/json")
        try:
            with urllib.request.urlopen(req, timeout=5) as resp:
                return resp.status, json.loads(resp.read().decode())
        except urllib.error.HTTPError as exc:
            return exc.code, json.loads(exc.read().decode())


BASE_PAYLOAD = {
    "audit_id": "API-001",
    "gate_period": 1000,
    "flows": [
        {"flow_id": "A", "priority": 0, "period": 1000,
         "transmit_time": 100, "deadline": 1000}
    ],
    "gate_entries": [{"start": 0, "end": 500, "priorities": [0]}],
}


class ApiTests(unittest.TestCase):
    def test_health_and_page(self):
        with HttpServerFixture() as srv:
            status, body = srv.call("GET", "/api/health")
            self.assertEqual(status, 200)
            self.assertTrue(body["ok"])
            req = urllib.request.Request(srv.base + "/")
            with urllib.request.urlopen(req, timeout=5) as resp:
                self.assertEqual(resp.status, 200)
                self.assertIn("冻结", resp.read().decode())

    def test_submit_freeze_read_idempotent_conflict(self):
        with HttpServerFixture() as srv:
            status, body = srv.call("POST", "/api/submit", BASE_PAYLOAD)
            self.assertEqual(status, 201)
            self.assertTrue(body["created"])
            h1 = body["content_hash"]
            self.assertEqual(body["decision"]["verdict"], "SCHEDULABLE")

            # 完全相同的提交 -> 同一冻结结论。
            status, body = srv.call("POST", "/api/submit", BASE_PAYLOAD)
            self.assertEqual(status, 200)
            self.assertFalse(body["created"])
            self.assertEqual(body["content_hash"], h1)

            # 读取接口返回同一冻结结论。
            status, body = srv.call("GET", "/api/decisions/API-001")
            self.assertEqual(status, 200)
            self.assertEqual(body["content_hash"], h1)
            self.assertTrue(body["frozen"])

            # 同一标识不同内容 -> 409，原裁决不变。
            changed = json.loads(json.dumps(BASE_PAYLOAD))
            changed["gate_entries"][0]["end"] = 400
            status, body = srv.call("POST", "/api/submit", changed)
            self.assertEqual(status, 409)
            self.assertEqual(body["error"], "AUDIT_CONFLICT")
            self.assertNotEqual(body["stored_hash"], body["incoming_hash"])
            self.assertEqual(body["existing_decision"]["content_hash"], h1)

            status, body = srv.call("GET", "/api/decisions/API-001")
            self.assertEqual(body["content_hash"], h1)
            self.assertEqual(
                body["request"]["gate_entries"][0]["end"], 500
            )

    def test_validation_error_and_missing(self):
        with HttpServerFixture() as srv:
            bad = json.loads(json.dumps(BASE_PAYLOAD))
            bad["audit_id"] = "API-BAD"
            bad["flows"] = []
            status, body = srv.call("POST", "/api/submit", bad)
            self.assertEqual(status, 400)
            self.assertEqual(body["error"], "VALIDATION_FAILED")

            status, body = srv.call("GET", "/api/decisions/UNKNOWN")
            self.assertEqual(status, 404)

            # 非 JSON 请求体 -> 400。
            req = urllib.request.Request(
                srv.base + "/api/submit",
                data=b"not-json",
                method="POST",
            )
            req.add_header("Content-Type", "application/json")
            try:
                urllib.request.urlopen(req, timeout=5)
                self.fail("应返回 400")
            except urllib.error.HTTPError as exc:
                self.assertEqual(exc.code, 400)

    def test_non_convergent_payload_via_api(self):
        with HttpServerFixture() as srv:
            payload = {
                "audit_id": "API-GROW",
                "gate_period": 1000,
                "flows": [
                    {"flow_id": "X", "priority": 0, "period": 1000,
                     "transmit_time": 900, "deadline": 1000}
                ],
                "gate_entries": [{"start": 0, "end": 100, "priorities": [0]}],
            }
            status, body = srv.call("POST", "/api/submit", payload)
            self.assertEqual(status, 201)
            self.assertEqual(body["decision"]["verdict"], "NON_CONVERGENT")
            chain = body["decision"]["growth_chain"]
            self.assertGreaterEqual(len(chain), 2)

    def test_wait_chain_endpoint(self):
        miss = {
            "audit_id": "API-WAIT",
            "gate_period": 1000,
            "flows": [
                {"flow_id": "HI", "priority": 0, "period": 1000,
                 "transmit_time": 600, "deadline": 1000},
                {"flow_id": "LO", "priority": 1, "period": 2000,
                 "transmit_time": 300, "deadline": 800},
            ],
            "gate_entries": [
                {"start": 0, "end": 600, "priorities": [0]},
                {"start": 600, "end": 1000, "priorities": [1]},
            ],
        }
        with HttpServerFixture() as srv:
            status, body = srv.call("POST", "/api/submit", miss)
            self.assertEqual(status, 201)
            self.assertEqual(body["decision"]["verdict"], "DEADLINE_MISS")
            frozen_hash = body["content_hash"]
            # 裁决主体带实例索引但不内嵌等待链大数据（保持读取兼容）。
            self.assertIn("instances", body["decision"])
            self.assertNotIn("wait_chains", body["decision"])

            status, w = srv.call("GET", "/api/decisions/API-WAIT/waits/LO/0")
            self.assertEqual(status, 200)
            self.assertTrue(w["ok"])
            self.assertEqual(w["content_hash"], frozen_hash)
            self.assertEqual(w["frame"]["frame"], "LO#0")
            self.assertTrue(w["frame"]["started"])
            iv = w["wait"]["intervals"]
            # LO#0 在 [0,600) 被 HI#0 占用，600 开始发送：单段、连续覆盖。
            self.assertEqual([(s["from"], s["to"]) for s in iv], [(0, 600)])
            self.assertEqual(w["wait"]["to"], 600)
            self.assertEqual(iv[0]["blocker"], "higher_priority_tx")
            self.assertEqual(iv[0]["source"]["frame"], "HI#0")
            self.assertEqual(iv[0]["source"]["flow_id"], "HI")
            self.assertEqual(iv[0]["source"]["release"], 0)
            for s in iv:
                self.assertIn("queue_length", s)
                self.assertIn("gate_open", s)
                self.assertIn("eligible_priority", s)

            # 不存在的实例 -> 404 INSTANCE_NOT_FOUND。
            status, w = srv.call("GET", "/api/decisions/API-WAIT/waits/LO/9")
            self.assertEqual(status, 404)
            self.assertEqual(w["error"], "INSTANCE_NOT_FOUND")
            status, w = srv.call("GET", "/api/decisions/API-WAIT/waits/NOPE/0")
            self.assertEqual(status, 404)
            self.assertEqual(w["error"], "INSTANCE_NOT_FOUND")
            # 不存在的裁决 -> 404 NOT_FOUND。
            status, w = srv.call("GET", "/api/decisions/NOPE/waits/LO/0")
            self.assertEqual(status, 404)
            self.assertEqual(w["error"], "NOT_FOUND")
            # 非法实例序号 -> 400。
            status, w = srv.call("GET", "/api/decisions/API-WAIT/waits/LO/abc")
            self.assertEqual(status, 400)
            self.assertEqual(w["error"], "INVALID_INSTANCE")

            # 查询不改写冻结结果。
            status, again = srv.call("GET", "/api/decisions/API-WAIT")
            self.assertEqual(status, 200)
            self.assertEqual(again["content_hash"], frozen_hash)
            self.assertEqual(again["decision"]["verdict"], "DEADLINE_MISS")

    def test_wait_chain_unsent_frame_via_api(self):
        # 门从不为 p1 开放：B#0 超期且未开始，证据精确覆盖至截止期。
        payload = {
            "audit_id": "API-WAIT-UNSENT",
            "gate_period": 1000,
            "flows": [
                {"flow_id": "A", "priority": 0, "period": 1000,
                 "transmit_time": 100, "deadline": 1000},
                {"flow_id": "B", "priority": 1, "period": 1000,
                 "transmit_time": 100, "deadline": 1000},
            ],
            "gate_entries": [{"start": 0, "end": 200, "priorities": [0]}],
        }
        with HttpServerFixture() as srv:
            status, body = srv.call("POST", "/api/submit", payload)
            self.assertEqual(status, 201)
            self.assertEqual(body["decision"]["verdict"], "NON_CONVERGENT")
            status, w = srv.call("GET", "/api/decisions/API-WAIT-UNSENT/waits/B/0")
            self.assertEqual(status, 200)
            self.assertEqual(w["wait"]["outcome"], "unsent")
            self.assertEqual((w["wait"]["from"], w["wait"]["to"]), (0, 1000))
            self.assertFalse(w["frame"]["started"])
            self.assertTrue(w["frame"]["unsent_at_deadline"])
            iv = w["wait"]["intervals"]
            for a, b in zip(iv, iv[1:]):
                self.assertEqual(a["to"], b["from"])
            self.assertEqual(iv[-1]["to"], 1000)


if __name__ == "__main__":
    unittest.main()
